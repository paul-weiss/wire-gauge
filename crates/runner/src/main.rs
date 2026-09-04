//! The wire-gauge runner: `rtt <backend>` orchestrates one run — spawn this
//! same binary as the echo child, wait for its READY line, connect, drive
//! the scenario, emit one JSONL record.

use std::io::{self, BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use clap::Parser;
use harness::report::RunRecord;
use harness::scenario::{run_rtt, RttConfig, SEQ_HEADER_BYTES};

#[derive(Parser)]
#[command(
    name = "wire-gauge",
    about = "Message-transport benchmarks for trading-shaped workloads"
)]
enum Cli {
    /// Run the 1→1 round-trip scenario against a backend.
    Rtt(RttArgs),
    /// Internal: run as the echo peer. Spawned by `rtt`.
    #[command(hide = true)]
    Echo(EchoArgs),
}

#[derive(Parser)]
struct RttArgs {
    /// Backend to measure: shm, iceoryx2, aeron-ipc, aeron-udp, uds, tcp, udp, nats, jetstream, redis, kafka
    backend: String,
    /// Offered load, messages per second.
    #[arg(long, default_value_t = 10_000)]
    rate: u64,
    /// Message size in bytes (>= 8 for the sequence header).
    #[arg(long, default_value_t = 128)]
    size: usize,
    /// Measured seconds.
    #[arg(long, default_value_t = 10)]
    duration: u64,
    /// Discarded lead-in seconds.
    #[arg(long, default_value_t = 2)]
    warmup: u64,
    /// Append the JSONL record here as well as printing it.
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Parser)]
struct EchoArgs {
    backend: String,
    #[arg(long)]
    bind: String,
    #[arg(long)]
    size: usize,
}

mod broker;

fn main() -> io::Result<()> {
    match Cli::parse() {
        Cli::Echo(args) => dispatch::echo(&args.backend, &args.bind, args.size),
        Cli::Rtt(args) => rtt(args),
    }
}

/// One match per backend crate; new backends get a line in each function.
mod dispatch {
    use crate::broker;
    use harness::transport::{Receiver, Sender};
    use std::io;

    pub const BACKENDS: &[&str] = &[
        "shm",
        "iceoryx2",
        "aeron-ipc",
        "aeron-udp",
        "uds",
        "tcp",
        "udp",
        "nats",
        "jetstream",
        "redis",
        "kafka",
    ];

    fn aeron_mode(backend: &str) -> backend_aeron::Mode {
        match backend {
            "aeron-ipc" => backend_aeron::Mode::Ipc,
            _ => backend_aeron::Mode::Udp,
        }
    }

    pub fn default_bind(backend: &str) -> io::Result<String> {
        match backend {
            "shm" => Ok(backend_shm::default_bind()),
            "iceoryx2" => Ok(backend_iceoryx2::default_bind()),
            "aeron-ipc" | "aeron-udp" => Ok(backend_aeron::default_bind(aeron_mode(backend))),
            "nats" | "jetstream" => Ok(backend_nats::default_bind(broker::NATS_PORT)),
            "redis" => Ok(backend_redis::default_bind(broker::REDIS_PORT)),
            "kafka" => Ok(backend_kafka::default_bind(broker::KAFKA_PORT)),
            _ => backend_sockets::default_bind(backend),
        }
    }

    pub fn echo(backend: &str, bind: &str, msg_size: usize) -> io::Result<()> {
        match backend {
            "shm" => backend_shm::echo(bind, msg_size),
            "iceoryx2" => backend_iceoryx2::echo(bind, msg_size),
            "aeron-ipc" | "aeron-udp" => backend_aeron::echo(aeron_mode(backend), bind, msg_size),
            "nats" => backend_nats::echo(bind, msg_size),
            "jetstream" => backend_nats::echo_js(bind, msg_size),
            "redis" => backend_redis::echo(bind, msg_size),
            "kafka" => backend_kafka::echo(bind, msg_size),
            _ => backend_sockets::echo(backend, bind, msg_size),
        }
    }

    pub fn connect(
        backend: &str,
        addr: &str,
        msg_size: usize,
    ) -> io::Result<(Box<dyn Sender>, Box<dyn Receiver>)> {
        match backend {
            "shm" => backend_shm::connect(addr, msg_size),
            "iceoryx2" => backend_iceoryx2::connect(addr, msg_size),
            "aeron-ipc" | "aeron-udp" => {
                backend_aeron::connect(aeron_mode(backend), addr, msg_size)
            }
            "nats" => backend_nats::connect(addr, msg_size),
            "jetstream" => backend_nats::connect_js(addr, msg_size),
            "redis" => backend_redis::connect(addr, msg_size),
            "kafka" => backend_kafka::connect(addr, msg_size),
            _ => backend_sockets::connect(backend, addr, msg_size),
        }
    }

    /// Files the run leaves behind that the orchestrator must remove.
    pub fn cleanup_paths(backend: &str, bind: &str) -> Vec<std::path::PathBuf> {
        match backend {
            "shm" => backend_shm::ring_paths(bind).into_iter().collect(),
            "aeron-ipc" | "aeron-udp" => vec![std::path::PathBuf::from(bind)],
            "uds" => vec![std::path::PathBuf::from(bind)],
            _ => Vec::new(),
        }
    }
}

fn rtt(args: RttArgs) -> io::Result<()> {
    if args.size < SEQ_HEADER_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("--size must be at least {SEQ_HEADER_BYTES}"),
        ));
    }
    if !dispatch::BACKENDS.contains(&args.backend.as_str()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "unknown backend '{}' (have: {})",
                args.backend,
                dispatch::BACKENDS.join(", ")
            ),
        ));
    }
    let bind = dispatch::default_bind(&args.backend)?;
    let mut broker = broker::Broker::start_for(&args.backend)?;
    let mut child = EchoChild::spawn(&args.backend, &bind, args.size)?;

    let (tx, rx) = dispatch::connect(&args.backend, &child.addr, args.size)?;
    let cfg = RttConfig {
        rate: args.rate,
        msg_size: args.size,
        duration: Duration::from_secs(args.duration),
        warmup: Duration::from_secs(args.warmup),
    };
    eprintln!(
        "wire-gauge rtt-1to1: backend={} rate={}/s size={}B duration={}s warmup={}s",
        args.backend, cfg.rate, cfg.msg_size, args.duration, args.warmup
    );
    let outcome = run_rtt(tx, rx, &cfg)?;
    child.stop();
    if let Some(b) = broker.as_mut() {
        b.stop();
    }

    let record = RunRecord::for_rtt(&args.backend, &cfg, &outcome);
    println!("{}", record.to_json_line());
    if let Some(path) = &args.out {
        record.append_to(path)?;
        eprintln!("appended to {}", path.display());
    }
    if record.results.dropped > 0 {
        eprintln!(
            "note: {} of {} messages dropped ({:.3}%)",
            record.results.dropped,
            record.results.sent,
            100.0 * record.results.dropped as f64 / record.results.sent as f64
        );
    }
    Ok(())
}

/// The spawned echo peer plus the address it announced.
struct EchoChild {
    child: Child,
    addr: String,
    cleanup: Vec<std::path::PathBuf>,
}

impl EchoChild {
    fn spawn(backend: &str, bind: &str, size: usize) -> io::Result<Self> {
        let exe = std::env::current_exe()?;
        let mut child = Command::new(exe)
            .args(["echo", backend, "--bind", bind, "--size", &size.to_string()])
            .stdout(Stdio::piped())
            .spawn()?;

        let stdout = child.stdout.take().expect("stdout was piped");
        let mut lines = BufReader::new(stdout).lines();
        let addr = loop {
            match lines.next() {
                Some(Ok(line)) => {
                    if let Some(addr) = line.strip_prefix("READY ") {
                        break addr.to_string();
                    }
                }
                Some(Err(e)) => return Err(e),
                None => {
                    return Err(io::Error::other(
                        "echo child exited before announcing READY",
                    ))
                }
            }
        };
        Ok(Self {
            child,
            addr,
            cleanup: dispatch::cleanup_paths(backend, bind),
        })
    }

    fn stop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
        for path in &self.cleanup {
            if path.is_dir() {
                std::fs::remove_dir_all(path).ok();
            } else {
                std::fs::remove_file(path).ok();
            }
        }
    }
}

impl Drop for EchoChild {
    fn drop(&mut self) {
        self.stop();
    }
}
