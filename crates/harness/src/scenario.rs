//! Scenarios drive a transport through a workload and measure it.
//!
//! The one that exists today is `rtt_1to1`: the order-path shape. A sender
//! fires fixed-size messages at a scheduled rate at an echo peer and the
//! receive thread records round-trip latency **from the intended send time**,
//! not the actual one — the coordinated-omission rule from REQUIREMENTS.md.
//! Sends are never gated on responses; any number of messages may be in
//! flight, exactly as a real order gateway backs up under load.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

use crate::clock::spin_sleep_until;
use crate::transport::{Receiver, RecvOutcome, Sender};

/// Sequence numbers occupy the first 8 bytes of every message.
pub const SEQ_HEADER_BYTES: usize = 8;

/// One hour in nanoseconds — histogram upper bound. Anything slower than
/// this is a hang, not a latency.
const HIST_MAX_NS: u64 = 3_600_000_000_000;

#[derive(Debug, Clone)]
pub struct RttConfig {
    /// Offered load, messages per second.
    pub rate: u64,
    /// Message size in bytes, including the 8-byte sequence header.
    pub msg_size: usize,
    /// Measured portion of the run.
    pub duration: Duration,
    /// Discarded lead-in. Warmup messages are sent and echoed identically
    /// but excluded from both histograms.
    pub warmup: Duration,
}

pub struct RttOutcome {
    pub sent: u64,
    pub received: u64,
    /// Received messages that fell inside the measured window.
    pub measured: u64,
    /// Round-trip latency from intended send time, nanoseconds.
    pub latency: Histogram<u64>,
    /// How late each send left relative to its schedule, nanoseconds. A
    /// diagnostic: if this grows, the generator (not the transport) is the
    /// bottleneck and the run is invalid at this rate.
    pub send_lag: Histogram<u64>,
    pub elapsed: Duration,
}

struct RxStats {
    latency: Histogram<u64>,
    received: u64,
    measured: u64,
}

fn new_hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, HIST_MAX_NS, 3).expect("static bounds are valid")
}

/// Run the 1→1 round-trip scenario over an already-connected transport whose
/// peer echoes every message back verbatim.
pub fn run_rtt<S, R>(mut tx: S, mut rx: R, cfg: &RttConfig) -> io::Result<RttOutcome>
where
    S: Sender,
    R: Receiver + 'static,
{
    assert!(cfg.rate > 0, "rate must be positive");
    assert!(
        cfg.msg_size >= SEQ_HEADER_BYTES,
        "msg_size must hold the {SEQ_HEADER_BYTES}-byte sequence header"
    );

    let interval_ns = 1_000_000_000 / cfg.rate;
    let warmup_msgs = cfg.rate * cfg.warmup.as_secs();
    let total = warmup_msgs + cfg.rate * cfg.duration.as_secs();

    // Small lead so message 0 isn't already late when the threads spin up.
    let schedule_start = Instant::now() + Duration::from_millis(10);
    let tx_done = Arc::new(AtomicBool::new(false));

    let rx_thread = {
        let tx_done = Arc::clone(&tx_done);
        let msg_size = cfg.msg_size;
        thread::spawn(move || -> io::Result<RxStats> {
            let mut stats = RxStats {
                latency: new_hist(),
                received: 0,
                measured: 0,
            };
            let mut buf = vec![0u8; msg_size];
            while stats.received < total {
                match rx.recv(&mut buf)? {
                    RecvOutcome::Msg => {
                        let now = Instant::now();
                        stats.received += 1;
                        let seq = u64::from_le_bytes(buf[..8].try_into().unwrap());
                        if seq >= warmup_msgs {
                            let intended = schedule_start + Duration::from_nanos(seq * interval_ns);
                            let ns = now.saturating_duration_since(intended).as_nanos() as u64;
                            stats.latency.saturating_record(ns.max(1));
                            stats.measured += 1;
                        }
                    }
                    // A timeout after the sender finished is the drain phase
                    // ending: on a lossy transport the missing messages are
                    // never coming. Before that it's just a quiet second.
                    RecvOutcome::TimedOut => {
                        if tx_done.load(Ordering::Acquire) {
                            break;
                        }
                    }
                    RecvOutcome::Closed => break,
                }
            }
            Ok(stats)
        })
    };

    let mut send_lag = new_hist();
    let mut buf = vec![0u8; cfg.msg_size];
    let started = Instant::now();
    for seq in 0..total {
        let target = schedule_start + Duration::from_nanos(seq * interval_ns);
        spin_sleep_until(target);
        let lag_ns = Instant::now().saturating_duration_since(target).as_nanos() as u64;
        buf[..8].copy_from_slice(&seq.to_le_bytes());
        tx.send(&buf)?;
        send_lag.saturating_record(lag_ns.max(1));
    }
    tx_done.store(true, Ordering::Release);

    let rx_stats = rx_thread
        .join()
        .map_err(|_| io::Error::other("receive thread panicked"))??;
    let elapsed = started.elapsed();

    Ok(RttOutcome {
        sent: total,
        received: rx_stats.received,
        measured: rx_stats.measured,
        latency: rx_stats.latency,
        send_lag,
        elapsed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    /// In-memory instant echo: whatever is sent appears on the receiver.
    /// Exercises the scenario machinery with zero transport under it.
    struct ChanTx(mpsc::Sender<Vec<u8>>);
    struct ChanRx(mpsc::Receiver<Vec<u8>>);

    impl Sender for ChanTx {
        fn send(&mut self, msg: &[u8]) -> io::Result<()> {
            self.0.send(msg.to_vec()).map_err(io::Error::other)
        }
    }

    impl Receiver for ChanRx {
        fn recv(&mut self, buf: &mut [u8]) -> io::Result<RecvOutcome> {
            match self.0.recv_timeout(Duration::from_millis(100)) {
                Ok(msg) => {
                    buf.copy_from_slice(&msg);
                    Ok(RecvOutcome::Msg)
                }
                Err(mpsc::RecvTimeoutError::Timeout) => Ok(RecvOutcome::TimedOut),
                Err(mpsc::RecvTimeoutError::Disconnected) => Ok(RecvOutcome::Closed),
            }
        }
    }

    #[test]
    fn rtt_over_instant_echo_accounts_for_every_message() {
        let (tx, rx) = mpsc::channel();
        let cfg = RttConfig {
            rate: 5_000,
            msg_size: 64,
            duration: Duration::from_secs(1),
            warmup: Duration::from_secs(1),
        };
        let outcome = run_rtt(ChanTx(tx), ChanRx(rx), &cfg).unwrap();
        assert_eq!(outcome.sent, 10_000);
        assert_eq!(outcome.received, 10_000);
        assert_eq!(outcome.measured, 5_000, "warmup half must be excluded");
        assert_eq!(outcome.latency.len(), 5_000);
        assert_eq!(
            outcome.send_lag.len(),
            10_000,
            "lag is recorded for every send"
        );
    }
}
