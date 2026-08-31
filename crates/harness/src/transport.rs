//! The transport abstraction every backend implements.
//!
//! Messages are fixed-size per run, so stream transports frame by length
//! alone (read exactly `msg_size` bytes) and datagram transports map one
//! message to one datagram. The first 8 bytes of every message carry the
//! sequence number, little-endian; the harness owns that header, backends
//! treat the payload as opaque.

use std::io;

/// Result of one receive attempt.
#[derive(Debug, PartialEq, Eq)]
pub enum RecvOutcome {
    /// One whole message landed in the buffer.
    Msg,
    /// The backend's read timeout elapsed with no (complete) message. The
    /// scenario decides whether that means "keep draining" or "we're done".
    TimedOut,
    /// The peer closed cleanly.
    Closed,
}

/// Sending half of a connection. Stays on the thread that created it —
/// deliberately not `Send`, because some backends (iceoryx2) have
/// single-threaded ports and the harness never moves the sender anyway.
pub trait Sender {
    /// Send one whole message. Blocking; a short write is an error.
    fn send(&mut self, msg: &[u8]) -> io::Result<()>;
}

/// Receiving half of a connection. Must be usable from its own thread.
///
/// Implementations are expected to carry a read timeout on the order of a
/// second so the scenario's drain phase can terminate on lossy transports.
pub trait Receiver: Send {
    /// Receive exactly one message into `buf` (whose length is `msg_size`).
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<RecvOutcome>;
}

impl Sender for Box<dyn Sender> {
    fn send(&mut self, msg: &[u8]) -> io::Result<()> {
        (**self).send(msg)
    }
}

impl Receiver for Box<dyn Receiver> {
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<RecvOutcome> {
        (**self).recv(buf)
    }
}
