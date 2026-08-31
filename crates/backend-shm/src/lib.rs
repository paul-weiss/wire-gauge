//! The custom shared-memory ring — the floor every other transport is
//! measured against (REQUIREMENTS.md hypothesis: 0.1–0.5µs).
//!
//! Design: one file-backed `MAP_SHARED` mapping per direction, each holding a
//! single-producer/single-consumer ring in the LMAX style. A slot is an
//! 8-byte sequence stamp plus the fixed-size payload, padded to a cache-line
//! multiple. The producer copies the payload then publishes the stamp with a
//! Release store; the consumer spins on the stamp of the slot it expects
//! with Acquire loads. The stamp stores `seq + 1`, so a zero-filled fresh
//! file means "nothing published" for every slot on every lap.
//!
//! Flow control is backpressure, not overwrite: the consumer publishes its
//! cursor in the header and a full producer spins until space opens. That
//! choice makes a slow consumer show up as *send lag* in the harness — the
//! honest place for it — instead of as silent message loss.
//!
//! Two files, `<base>.c2s` and `<base>.s2c`, give the echo pair one ring per
//! direction. The echo child creates and initializes both before announcing
//! READY, so the connecting side never observes a half-built ring.

use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use memmap2::MmapMut;

use harness::transport::{Receiver, RecvOutcome, Sender};

const MAGIC: u64 = 0x5749_5245_4741_5547; // "WIREGAUG"
const CACHE_LINE: usize = 64;
const HEADER_BYTES: usize = 4096; // one page keeps slots page-aligned
const MAGIC_OFF: usize = 0;
const MSG_SIZE_OFF: usize = 8;
const CAPACITY_OFF: usize = 16;
/// Consumer cursor lives on its own cache line so producer polling of it
/// never contends with the static header fields.
const CURSOR_OFF: usize = 64;

/// Slots per ring. 64K slots of a 128B message ≈ 12MB per direction —
/// big enough that backpressure only engages when the consumer truly stalls.
const CAPACITY: u64 = 65_536;

const RECV_TIMEOUT: Duration = Duration::from_secs(1);
/// A producer stuck this long on a full ring means the peer is gone.
const FULL_TIMEOUT: Duration = Duration::from_secs(5);

pub fn default_bind() -> String {
    std::env::temp_dir()
        .join(format!("wire-gauge-{}-shm", std::process::id()))
        .to_string_lossy()
        .into_owned()
}

/// Both ring files for a base path; the connect side removes them at the end.
pub fn ring_paths(base: &str) -> [PathBuf; 2] {
    [
        PathBuf::from(format!("{base}.c2s")),
        PathBuf::from(format!("{base}.s2c")),
    ]
}

pub fn echo(base: &str, msg_size: usize) -> io::Result<()> {
    let [c2s, s2c] = ring_paths(base);
    let mut rx = RingConsumer::open(Ring::create(&c2s, msg_size)?);
    let mut tx = RingProducer::open(Ring::create(&s2c, msg_size)?);
    println!("READY {base}");
    use std::io::Write;
    io::stdout().flush()?;

    let mut buf = vec![0u8; msg_size];
    loop {
        match rx.recv(&mut buf)? {
            RecvOutcome::Msg => tx.send(&buf)?,
            RecvOutcome::TimedOut => {} // quiet stretch; keep serving until killed
            RecvOutcome::Closed => unreachable!("shm rings never close"),
        }
    }
}

pub fn connect(base: &str, msg_size: usize) -> io::Result<(Box<dyn Sender>, Box<dyn Receiver>)> {
    let [c2s, s2c] = ring_paths(base);
    let tx = RingProducer::open(Ring::open_existing(&c2s, msg_size)?);
    let rx = RingConsumer::open(Ring::open_existing(&s2c, msg_size)?);
    Ok((Box::new(tx), Box::new(rx)))
}

/// One mapped ring. Producer and consumer wrap this with their local state.
struct Ring {
    map: MmapMut,
    msg_size: usize,
    stride: usize,
}

// The raw pointers derived from `map` stay valid for the mapping's lifetime
// and every cross-process access goes through atomics; moving the struct to
// another thread moves the mapping wholesale.
unsafe impl Send for Ring {}

impl Ring {
    fn layout(msg_size: usize) -> (usize, usize) {
        let stride = (8 + msg_size).div_ceil(CACHE_LINE) * CACHE_LINE;
        (stride, HEADER_BYTES + stride * CAPACITY as usize)
    }

    fn create(path: &Path, msg_size: usize) -> io::Result<Self> {
        let (_, total) = Self::layout(msg_size);
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        file.set_len(total as u64)?; // zero-filled: every stamp reads "empty"
        let map = unsafe { MmapMut::map_mut(&file)? };
        let ring = Self {
            map,
            msg_size,
            stride: Self::layout(msg_size).0,
        };
        ring.prefault();
        ring.write_u64(MSG_SIZE_OFF, msg_size as u64);
        ring.write_u64(CAPACITY_OFF, CAPACITY);
        ring.write_u64(MAGIC_OFF, MAGIC);
        Ok(ring)
    }

    fn open_existing(path: &Path, msg_size: usize) -> io::Result<Self> {
        let (_, total) = Self::layout(msg_size);
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        if file.metadata()?.len() != total as u64 {
            return Err(io::Error::other("ring file has unexpected size"));
        }
        let map = unsafe { MmapMut::map_mut(&file)? };
        let ring = Self {
            map,
            msg_size,
            stride: Self::layout(msg_size).0,
        };
        if ring.read_u64(MAGIC_OFF) != MAGIC
            || ring.read_u64(MSG_SIZE_OFF) != msg_size as u64
            || ring.read_u64(CAPACITY_OFF) != CAPACITY
        {
            return Err(io::Error::other("ring header mismatch"));
        }
        ring.prefault();
        Ok(ring)
    }

    /// Touch every page with a write so the whole mapping is faulted in and
    /// dirty before the first message. Mappings fault per process, so both
    /// ends do this. Without it, a run that covers the ring's first lap eats
    /// a soft fault every page-boundary crossing — measured on primes as a
    /// 7µs p99 at 5k msgs/s (60k messages ≈ one lap of a 64K ring) against
    /// 0.4µs at 100k msgs/s, where 18 laps dilute the first one.
    ///
    /// Safe to run concurrently with a quiet peer: every touch rewrites the
    /// value it just read, and nothing flows until after both ends open.
    fn prefault(&self) {
        const PAGE: usize = 4096;
        let base = self.map.as_ptr() as *mut u64;
        for off in (0..self.map.len()).step_by(PAGE) {
            unsafe {
                let p = base.byte_add(off);
                p.write_volatile(p.read_volatile());
            }
        }
    }

    fn write_u64(&self, off: usize, v: u64) {
        self.atomic_at(off).store(v, Ordering::Release);
    }

    fn read_u64(&self, off: usize) -> u64 {
        self.atomic_at(off).load(Ordering::Acquire)
    }

    fn atomic_at(&self, off: usize) -> &AtomicU64 {
        debug_assert!(off + 8 <= self.map.len() && off.is_multiple_of(8));
        unsafe { &*(self.map.as_ptr().add(off) as *const AtomicU64) }
    }

    fn cursor(&self) -> &AtomicU64 {
        self.atomic_at(CURSOR_OFF)
    }

    fn stamp(&self, seq: u64) -> &AtomicU64 {
        let idx = (seq % CAPACITY) as usize;
        self.atomic_at(HEADER_BYTES + idx * self.stride)
    }

    fn payload_ptr(&self, seq: u64) -> *mut u8 {
        let idx = (seq % CAPACITY) as usize;
        unsafe { self.map.as_ptr().add(HEADER_BYTES + idx * self.stride + 8) as *mut u8 }
    }
}

pub struct RingProducer {
    ring: Ring,
    next_seq: u64,
    /// Consumer cursor as last observed; refreshed only when the ring looks
    /// full so the hot path doesn't touch the shared cursor line at all.
    cached_cursor: u64,
}

impl RingProducer {
    fn open(ring: Ring) -> Self {
        Self {
            ring,
            next_seq: 0,
            cached_cursor: 0,
        }
    }
}

impl Sender for RingProducer {
    fn send(&mut self, msg: &[u8]) -> io::Result<()> {
        debug_assert_eq!(msg.len(), self.ring.msg_size);
        if self.next_seq - self.cached_cursor >= CAPACITY {
            let deadline = Instant::now() + FULL_TIMEOUT;
            loop {
                self.cached_cursor = self.ring.cursor().load(Ordering::Acquire);
                if self.next_seq - self.cached_cursor < CAPACITY {
                    break;
                }
                if Instant::now() > deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "ring full and consumer not draining",
                    ));
                }
                std::hint::spin_loop();
            }
        }
        let seq = self.next_seq;
        unsafe {
            std::ptr::copy_nonoverlapping(msg.as_ptr(), self.ring.payload_ptr(seq), msg.len());
        }
        self.ring.stamp(seq).store(seq + 1, Ordering::Release);
        self.next_seq = seq + 1;
        Ok(())
    }
}

pub struct RingConsumer {
    ring: Ring,
    next_seq: u64,
}

impl RingConsumer {
    fn open(ring: Ring) -> Self {
        Self { ring, next_seq: 0 }
    }
}

impl Receiver for RingConsumer {
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<RecvOutcome> {
        debug_assert_eq!(buf.len(), self.ring.msg_size);
        let seq = self.next_seq;
        let stamp = self.ring.stamp(seq);
        // Keep Instant::now() off the wait loop: spin freely first, then
        // consult the clock only every SPIN_STRIDE misses. Checking it every
        // iteration showed up as p99 jitter at low rates (measured on primes:
        // 6.95µs vs 0.94µs for the strided iceoryx2 loop at 5k msgs/s).
        const SPIN_POLLS: u32 = 1_000;
        const SPIN_STRIDE: u32 = 256;
        let mut misses: u32 = 0;
        let mut deadline: Option<Instant> = None;
        while stamp.load(Ordering::Acquire) != seq + 1 {
            misses += 1;
            if misses >= SPIN_POLLS && misses.is_multiple_of(SPIN_STRIDE) {
                match deadline {
                    None => deadline = Some(Instant::now() + RECV_TIMEOUT),
                    Some(d) => {
                        if Instant::now() > d {
                            return Ok(RecvOutcome::TimedOut);
                        }
                    }
                }
            }
            std::hint::spin_loop();
        }
        unsafe {
            std::ptr::copy_nonoverlapping(self.ring.payload_ptr(seq), buf.as_mut_ptr(), buf.len());
        }
        self.ring.cursor().store(seq + 1, Ordering::Release);
        self.next_seq = seq + 1;
        Ok(RecvOutcome::Msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_base(tag: &str) -> String {
        std::env::temp_dir()
            .join(format!("wire-gauge-test-{}-{tag}", std::process::id()))
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn ring_round_trips_messages_in_order() {
        let base = temp_base("order");
        let path = PathBuf::from(format!("{base}.ring"));
        let msg_size = 64;
        let mut tx = RingProducer::open(Ring::create(&path, msg_size).unwrap());
        let mut rx = RingConsumer::open(Ring::open_existing(&path, msg_size).unwrap());

        let mut buf = vec![0u8; msg_size];
        for round in 0u64..3 {
            for i in 0u64..1_000 {
                let mut msg = vec![0u8; msg_size];
                msg[..8].copy_from_slice(&(round * 1_000 + i).to_le_bytes());
                tx.send(&msg).unwrap();
            }
            for i in 0u64..1_000 {
                assert_eq!(rx.recv(&mut buf).unwrap(), RecvOutcome::Msg);
                let seq = u64::from_le_bytes(buf[..8].try_into().unwrap());
                assert_eq!(seq, round * 1_000 + i);
            }
        }
        assert_eq!(rx.recv(&mut buf).unwrap(), RecvOutcome::TimedOut);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn wraps_the_capacity_boundary() {
        let base = temp_base("wrap");
        let path = PathBuf::from(format!("{base}.ring"));
        let msg_size = 16;
        let mut tx = RingProducer::open(Ring::create(&path, msg_size).unwrap());
        let mut rx = RingConsumer::open(Ring::open_existing(&path, msg_size).unwrap());

        // Interleave so the pair laps the ring twice without ever filling it.
        let total = CAPACITY * 2 + 17;
        let mut buf = vec![0u8; msg_size];
        for i in 0..total {
            let mut msg = vec![0u8; msg_size];
            msg[..8].copy_from_slice(&i.to_le_bytes());
            tx.send(&msg).unwrap();
            assert_eq!(rx.recv(&mut buf).unwrap(), RecvOutcome::Msg);
            assert_eq!(u64::from_le_bytes(buf[..8].try_into().unwrap()), i);
        }
        std::fs::remove_file(&path).unwrap();
    }
}
