//! Redis Streams — the "is good-enough good enough?" broker. A single-
//! threaded server, append-only stream per direction, blocking XREAD on the
//! consumer side.
//!
//! Configuration, for the report:
//! - The runner starts redis with `--save "" --appendonly no`: pure
//!   in-memory streams, no persistence in the data path. That is the
//!   *fastest honest* redis — turning AOF on only slows it down, so the
//!   numbers here are its best case.
//! - Streams are capped with approximate `MAXLEN ~ 200000` trimming so an
//!   unbounded run can't eat the box; trimming is off the hot path.
//! - Every XADD is a full client→server→client round trip (the sync client
//!   waits for the returned entry id). There is no fire-and-forget in the
//!   redis protocol short of pipelining, which would batch — so offered
//!   rates above what one round trip sustains will surface as send lag,
//!   which is the honest place for them.
//!
//! The echo child reads `<base>:c2s` and appends to `<base>:s2c`; the
//! measuring side does the reverse. Payload rides in a single binary field
//! `d`. Entry ids are server-assigned; consumers track the last-seen id and
//! start from 0, so a consumer that connects late replays rather than
//! losing messages — drops through this backend should be impossible.

use std::io;

use redis::streams::{StreamMaxlen, StreamReadOptions, StreamReadReply};
use redis::{Commands, Connection};

use harness::transport::{Receiver, RecvOutcome, Sender};

const MAXLEN: usize = 200_000;
const BLOCK_MS: usize = 1_000;

pub fn default_bind(port: u16) -> String {
    format!("127.0.0.1:{port}")
}

fn err(e: redis::RedisError) -> io::Error {
    io::Error::other(format!("redis: {e}"))
}

fn open(addr: &str) -> io::Result<Connection> {
    let client = redis::Client::open(format!("redis://{addr}/")).map_err(err)?;
    client.get_connection().map_err(err)
}

/// `addr` arrives as `host:port`; the announced address appends the key
/// base so both ends agree on stream names: `host:port/<base>`.
fn split_addr(addr: &str) -> (&str, &str) {
    match addr.split_once('/') {
        Some((hp, base)) => (hp, base),
        None => (addr, "wg"),
    }
}

pub fn echo(bind: &str, _msg_size: usize) -> io::Result<()> {
    let base = format!("wg-{}", std::process::id());
    let mut rx = StreamReceiver::open(bind, &format!("{base}:c2s"))?;
    let mut tx = StreamSender::open(bind, &format!("{base}:s2c"))?;

    println!("READY {bind}/{base}");
    use std::io::Write;
    io::stdout().flush()?;

    let mut buf = vec![0u8; 65_536];
    loop {
        // A None is a blocked read timing out; keep serving until killed.
        if let Some(n) = rx.recv_any(&mut buf)? {
            tx.send(&buf[..n])?;
        }
    }
}

pub fn connect(addr: &str, _msg_size: usize) -> io::Result<(Box<dyn Sender>, Box<dyn Receiver>)> {
    let (hp, base) = split_addr(addr);
    let tx = StreamSender::open(hp, &format!("{base}:c2s"))?;
    let rx = StreamReceiver::open(hp, &format!("{base}:s2c"))?;
    Ok((Box::new(tx), Box::new(rx)))
}

struct StreamSender {
    conn: Connection,
    key: String,
}

impl StreamSender {
    fn open(addr: &str, key: &str) -> io::Result<Self> {
        Ok(Self {
            conn: open(addr)?,
            key: key.to_string(),
        })
    }
}

impl Sender for StreamSender {
    fn send(&mut self, msg: &[u8]) -> io::Result<()> {
        let _id: String = self
            .conn
            .xadd_maxlen(&self.key, StreamMaxlen::Approx(MAXLEN), "*", &[("d", msg)])
            .map_err(err)?;
        Ok(())
    }
}

struct StreamReceiver {
    conn: Connection,
    key: String,
    last_id: String,
    /// Entries already fetched by one XREAD but not yet handed out.
    pending: Vec<(String, Vec<u8>)>,
}

impl StreamReceiver {
    fn open(addr: &str, key: &str) -> io::Result<Self> {
        Ok(Self {
            conn: open(addr)?,
            key: key.to_string(),
            last_id: "0".to_string(),
            pending: Vec::new(),
        })
    }

    /// One entry, blocking up to BLOCK_MS. None on timeout.
    fn recv_any(&mut self, buf: &mut [u8]) -> io::Result<Option<usize>> {
        if self.pending.is_empty() {
            let opts = StreamReadOptions::default().block(BLOCK_MS);
            let reply: StreamReadReply = self
                .conn
                .xread_options(&[&self.key], &[&self.last_id], &opts)
                .map_err(err)?;
            for stream_key in reply.keys {
                for entry in stream_key.ids {
                    let data: Vec<u8> = entry
                        .get("d")
                        .ok_or_else(|| io::Error::other("stream entry missing field d"))?;
                    self.pending.push((entry.id.clone(), data));
                }
            }
            // Oldest first for handout-by-pop from the back.
            self.pending.reverse();
        }
        match self.pending.pop() {
            Some((id, data)) => {
                self.last_id = id;
                if data.len() > buf.len() {
                    return Err(io::Error::other("stream entry larger than buffer"));
                }
                buf[..data.len()].copy_from_slice(&data);
                Ok(Some(data.len()))
            }
            None => Ok(None),
        }
    }
}

impl Receiver for StreamReceiver {
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<RecvOutcome> {
        match self.recv_any(buf)? {
            Some(n) if n == buf.len() => Ok(RecvOutcome::Msg),
            Some(n) => Err(io::Error::other(format!(
                "entry of {n} bytes, expected {}",
                buf.len()
            ))),
            None => Ok(RecvOutcome::TimedOut),
        }
    }
}
