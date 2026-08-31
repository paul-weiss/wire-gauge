//! The JSONL run record. One line per run; `results/` accumulates these and
//! the M5 report is generated from them. A number that can't be traced to a
//! record here doesn't go in the report.

use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hdrhistogram::Histogram;
use serde::Serialize;

use crate::scenario::{RttConfig, RttOutcome};

/// Bump on any breaking change to the record shape.
pub const SCHEMA: u32 = 1;

#[derive(Serialize)]
pub struct RunRecord {
    pub schema: u32,
    pub unix_time_s: u64,
    pub machine: Machine,
    pub backend: String,
    pub scenario: &'static str,
    pub config: RttConfigOut,
    pub results: RttResults,
}

#[derive(Serialize)]
pub struct Machine {
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub kernel: String,
    pub cpu: String,
}

#[derive(Serialize)]
pub struct RttConfigOut {
    pub rate: u64,
    pub msg_size: usize,
    pub duration_s: u64,
    pub warmup_s: u64,
}

#[derive(Serialize)]
pub struct RttResults {
    pub sent: u64,
    pub received: u64,
    pub dropped: u64,
    pub measured: u64,
    pub elapsed_s: f64,
    pub achieved_send_rate: f64,
    pub latency_ns: HistSummary,
    pub send_lag_ns: HistSummary,
}

#[derive(Serialize)]
pub struct HistSummary {
    pub count: u64,
    pub mean: f64,
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub p999: u64,
    pub p9999: u64,
    pub max: u64,
}

impl HistSummary {
    pub fn from_hist(h: &Histogram<u64>) -> Self {
        Self {
            count: h.len(),
            mean: h.mean(),
            p50: h.value_at_quantile(0.50),
            p90: h.value_at_quantile(0.90),
            p99: h.value_at_quantile(0.99),
            p999: h.value_at_quantile(0.999),
            p9999: h.value_at_quantile(0.9999),
            max: h.max(),
        }
    }
}

impl RunRecord {
    pub fn for_rtt(backend: &str, cfg: &RttConfig, out: &RttOutcome) -> Self {
        Self {
            schema: SCHEMA,
            unix_time_s: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_secs(),
            machine: Machine::detect(),
            backend: backend.to_string(),
            scenario: "rtt-1to1",
            config: RttConfigOut {
                rate: cfg.rate,
                msg_size: cfg.msg_size,
                duration_s: cfg.duration.as_secs(),
                warmup_s: cfg.warmup.as_secs(),
            },
            results: RttResults {
                sent: out.sent,
                received: out.received,
                dropped: out.sent - out.received,
                measured: out.measured,
                elapsed_s: out.elapsed.as_secs_f64(),
                achieved_send_rate: out.sent as f64 / out.elapsed.as_secs_f64(),
                latency_ns: HistSummary::from_hist(&out.latency),
                send_lag_ns: HistSummary::from_hist(&out.send_lag),
            },
        }
    }

    pub fn to_json_line(&self) -> String {
        serde_json::to_string(self).expect("record serializes")
    }

    /// Append this record as one line to `path`, creating parents as needed.
    pub fn append_to(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        writeln!(f, "{}", self.to_json_line())
    }
}

impl Machine {
    pub fn detect() -> Self {
        Self {
            hostname: cmd_line("hostname", &[]).unwrap_or_default(),
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            kernel: cmd_line("uname", &["-r"]).unwrap_or_default(),
            cpu: detect_cpu().unwrap_or_default(),
        }
    }
}

fn cmd_line(cmd: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(cmd).args(args).output().ok()?;
    let s = String::from_utf8_lossy(&out.stdout);
    Some(s.trim().to_string()).filter(|s| !s.is_empty())
}

fn detect_cpu() -> Option<String> {
    if cfg!(target_os = "macos") {
        cmd_line("sysctl", &["-n", "machdep.cpu.brand_string"])
    } else {
        let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").ok()?;
        cpuinfo
            .lines()
            .find(|l| l.starts_with("model name"))
            .and_then(|l| l.split(':').nth(1))
            .map(|s| s.trim().to_string())
    }
}
