//! Broker lifecycle for the brokered backends. The runner owns the broker
//! process the same way it owns the echo child: started before the echo
//! child, killed after it, nothing left running and no state left behind.
//!
//! Brokers run natively — REQUIREMENTS.md forbids Docker-on-Mac in the data
//! path — on non-default localhost ports so a developer's own redis/nats
//! is never touched. Every broker gets a throwaway work dir under the temp
//! dir; logs land there too, and the whole dir is removed on stop.
//!
//! Binary discovery: `$WG_<NAME>` env override first, then `$PATH`, then
//! the user-space install locations the provisioning notes use
//! (`~/bin`, `~/opt/kafka/bin`, the Homebrew kafka bindir).

use std::io;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub const NATS_PORT: u16 = 14222;
pub const REDIS_PORT: u16 = 16379;
pub const KAFKA_PORT: u16 = 19092;
const KAFKA_CONTROLLER_PORT: u16 = 19093;

pub struct Broker {
    child: Child,
    work_dir: PathBuf,
}

impl Broker {
    /// Start the broker a backend needs, or None for brokerless backends.
    ///
    /// `advertise` is the address a *remote* client will use to reach this
    /// broker (M6, cross-host). None keeps everything on loopback.
    pub fn start_for(backend: &str, advertise: Option<&str>) -> io::Result<Option<Broker>> {
        let listen_ip = if advertise.is_some() { "0.0.0.0" } else { "127.0.0.1" };
        let (port, ready_timeout) = match backend {
            "nats" | "jetstream" => (NATS_PORT, Duration::from_secs(10)),
            "redis" => (REDIS_PORT, Duration::from_secs(10)),
            "kafka" => (KAFKA_PORT, Duration::from_secs(60)),
            _ => return Ok(None),
        };
        let work_dir =
            std::env::temp_dir().join(format!("wire-gauge-broker-{}", std::process::id()));
        std::fs::create_dir_all(&work_dir)?;

        let mut cmd = match backend {
            "nats" => {
                let mut c = Command::new(find_bin("nats-server", "WG_NATS_SERVER")?);
                c.args(["-p", &NATS_PORT.to_string(), "-a", listen_ip]);
                c
            }
            "jetstream" => {
                let mut c = Command::new(find_bin("nats-server", "WG_NATS_SERVER")?);
                c.args([
                    "-p",
                    &NATS_PORT.to_string(),
                    "-a",
                    listen_ip,
                    "-js",
                    "-sd",
                ])
                .arg(work_dir.join("js"));
                c
            }
            "redis" => {
                let mut c = Command::new(find_bin("redis-server", "WG_REDIS_SERVER")?);
                c.args([
                    "--port",
                    &REDIS_PORT.to_string(),
                    "--bind",
                    listen_ip,
                    "--protected-mode",
                    if advertise.is_some() { "no" } else { "yes" },
                    "--save",
                    "",
                    "--appendonly",
                    "no",
                ])
                .current_dir(&work_dir);
                c
            }
            "kafka" => kafka_command(&work_dir, advertise)?,
            _ => unreachable!(),
        };

        let log = std::fs::File::create(work_dir.join("broker.log"))?;
        let child = cmd
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?;
        let mut broker = Broker { child, work_dir };
        broker.wait_ready(port, ready_timeout)?;
        Ok(Some(broker))
    }

    fn wait_ready(&mut self, port: u16, timeout: Duration) -> io::Result<()> {
        let deadline = Instant::now() + timeout;
        let addr = format!("127.0.0.1:{port}").parse().unwrap();
        loop {
            if let Some(status) = self.child.try_wait()? {
                let log =
                    std::fs::read_to_string(self.work_dir.join("broker.log")).unwrap_or_default();
                let tail: String = log.lines().rev().take(5).collect::<Vec<_>>().join(" | ");
                return Err(io::Error::other(format!(
                    "broker exited during startup ({status}): {tail}"
                )));
            }
            if TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("broker not accepting on port {port} after {timeout:?}"),
                ));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn stop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
        std::fs::remove_dir_all(&self.work_dir).ok();
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        self.stop();
    }
}

/// KRaft single-node kafka: write a properties file, format storage, start.
/// The format step runs to completion first; the returned command is the
/// server itself.
fn kafka_command(work_dir: &Path, advertise: Option<&str>) -> io::Result<Command> {
    let bindir = kafka_bindir()?;
    let props = work_dir.join("server.properties");
    // Kafka hands clients its advertised address on first contact, so a
    // remote client needs the host's reachable IP there, not loopback.
    let listen_ip = if advertise.is_some() { "0.0.0.0" } else { "127.0.0.1" };
    let advertised = advertise.unwrap_or("127.0.0.1");
    std::fs::write(
        &props,
        format!(
            "process.roles=broker,controller\n\
             node.id=1\n\
             controller.quorum.voters=1@127.0.0.1:{KAFKA_CONTROLLER_PORT}\n\
             listeners=PLAINTEXT://{listen_ip}:{KAFKA_PORT},CONTROLLER://127.0.0.1:{KAFKA_CONTROLLER_PORT}\n\
             advertised.listeners=PLAINTEXT://{advertised}:{KAFKA_PORT}\n\
             controller.listener.names=CONTROLLER\n\
             listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT\n\
             log.dirs={}\n\
             num.partitions=1\n\
             auto.create.topics.enable=true\n\
             group.initial.rebalance.delay.ms=0\n\
             offsets.topic.replication.factor=1\n\
             transaction.state.log.replication.factor=1\n\
             transaction.state.log.min.isr=1\n\
             share.coordinator.state.topic.replication.factor=1\n\
             share.coordinator.state.topic.min.isr=1\n",
            work_dir.join("data").display()
        ),
    )?;

    let storage = script(&bindir, "kafka-storage");
    let uuid_out = kafka_env(Command::new(&storage))
        .arg("random-uuid")
        .output()?;
    if !uuid_out.status.success() {
        return Err(io::Error::other(format!(
            "kafka-storage random-uuid failed: {}",
            String::from_utf8_lossy(&uuid_out.stderr)
        )));
    }
    let uuid = String::from_utf8_lossy(&uuid_out.stdout).trim().to_string();
    let fmt_out = kafka_env(Command::new(&storage))
        .args(["format", "-t", &uuid, "-c"])
        .arg(&props)
        .output()?;
    if !fmt_out.status.success() {
        return Err(io::Error::other(format!(
            "kafka-storage format failed: {}",
            String::from_utf8_lossy(&fmt_out.stderr)
        )));
    }

    let mut cmd = kafka_env(Command::new(script(&bindir, "kafka-server-start")));
    cmd.arg(&props);
    Ok(cmd)
}

/// Homebrew ships extensionless wrappers; the Apache tarball ships `.sh`.
fn script(bindir: &Path, name: &str) -> PathBuf {
    let plain = bindir.join(name);
    if plain.exists() {
        plain
    } else {
        bindir.join(format!("{name}.sh"))
    }
}

fn kafka_env(mut cmd: Command) -> Command {
    // The user-space JRE from provisioning, when java isn't on PATH.
    let jre = home().join("opt/jre");
    if jre.join("bin/java").exists() && which("java").is_none() {
        cmd.env("JAVA_HOME", &jre);
    }
    cmd.env("KAFKA_HEAP_OPTS", "-Xms1G -Xmx1G");
    cmd
}

fn kafka_bindir() -> io::Result<PathBuf> {
    if let Ok(dir) = std::env::var("WG_KAFKA_BIN") {
        return Ok(PathBuf::from(dir));
    }
    for cand in [
        home().join("opt/kafka/bin"),
        PathBuf::from("/opt/homebrew/opt/kafka/bin"),
        PathBuf::from("/usr/local/opt/kafka/bin"),
    ] {
        if script(&cand, "kafka-server-start").exists() {
            return Ok(cand);
        }
    }
    if let Some(found) = which("kafka-server-start").or_else(|| which("kafka-server-start.sh")) {
        return Ok(found.parent().unwrap().to_path_buf());
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "kafka not found (set WG_KAFKA_BIN to its bin directory)",
    ))
}

fn find_bin(name: &str, env_override: &str) -> io::Result<PathBuf> {
    if let Ok(p) = std::env::var(env_override) {
        return Ok(PathBuf::from(p));
    }
    if let Some(p) = which(name) {
        return Ok(p);
    }
    let user = home().join("bin").join(name);
    if user.exists() {
        return Ok(user);
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!("{name} not found (set {env_override} to its path)"),
    ))
}

fn which(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|d| d.join(name))
        .find(|p| p.exists())
}

fn home() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default()
}
