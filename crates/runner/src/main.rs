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
    /// Backend to measure: uds | tcp | udp
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

fn main() -> io::Result<()> {
    match Cli::parse() {
        Cli::Echo(args) => backend_sockets::echo(&args.backend, &args.bind, args.size),
        Cli::Rtt(args) => rtt(args),
    }
}

fn rtt(args: RttArgs) -> io::Result<()> {
    if args.size < SEQ_HEADER_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("--size must be at least {SEQ_HEADER_BYTES}"),
        ));
    }
    let bind = backend_sockets::default_bind(&args.backend)?;
    let mut child = EchoChild::spawn(&args.backend, &bind, args.size)?;

    let (tx, rx) = backend_sockets::connect(&args.backend, &child.addr, args.size)?;
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
    uds_path: Option<String>,
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
            uds_path: (backend == "uds").then(|| bind.to_string()),
        })
    }

    fn stop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
        if let Some(path) = &self.uds_path {
            std::fs::remove_file(path).ok();
        }
    }
}

impl Drop for EchoChild {
    fn drop(&mut self) {
        self.stop();
    }
}
