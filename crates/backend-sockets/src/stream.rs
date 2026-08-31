//! Shared machinery for byte-stream transports (TCP, UDS stream sockets).
//! Messages are fixed-size, so framing is "read exactly `msg_size` bytes".

use std::io::{self, Read, Write};

use harness::transport::{Receiver, RecvOutcome, Sender};

pub struct StreamSender<W: Write + Send>(pub W);

impl<W: Write + Send> Sender for StreamSender<W> {
    fn send(&mut self, msg: &[u8]) -> io::Result<()> {
        self.0.write_all(msg)
    }
}

pub struct StreamReceiver<R: Read + Send>(pub R);

impl<R: Read + Send> Receiver for StreamReceiver<R> {
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<RecvOutcome> {
        let mut filled = 0;
        let mut timeouts_mid_message = 0;
        while filled < buf.len() {
            match self.0.read(&mut buf[filled..]) {
                Ok(0) => {
                    return if filled == 0 {
                        Ok(RecvOutcome::Closed)
                    } else {
                        Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "peer closed mid-message",
                        ))
                    };
                }
                Ok(n) => filled += n,
                // macOS reports an SO_RCVTIMEO expiry as WouldBlock, Linux
                // as TimedOut; treat them identically.
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) =>
                {
                    if filled == 0 {
                        return Ok(RecvOutcome::TimedOut);
                    }
                    // A timeout with half a message in hand means the peer
                    // stalled mid-frame. Give it one more timeout window,
                    // then treat the connection as broken rather than
                    // spinning forever.
                    timeouts_mid_message += 1;
                    if timeouts_mid_message > 1 {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "peer stalled mid-message",
                        ));
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(RecvOutcome::Msg)
    }
}

/// The echo side's loop: read one message, write it back, until the peer
/// disconnects. Used by both stream backends with a blocking socket.
pub fn echo_loop<S: Read + Write>(mut sock: S, msg_size: usize) -> io::Result<()> {
    let mut buf = vec![0u8; msg_size];
    loop {
        let mut filled = 0;
        while filled < msg_size {
            match sock.read(&mut buf[filled..]) {
                Ok(0) => return Ok(()), // peer done; mid-message close = run over
                Ok(n) => filled += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                // The orchestrator kills us without ceremony; a reset here
                // is the normal end of a run, not a failure.
                Err(e) if e.kind() == io::ErrorKind::ConnectionReset => return Ok(()),
                Err(e) => return Err(e),
            }
        }
        sock.write_all(&buf)?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receiver_frames_fixed_size_messages_and_sees_close() {
        let two_messages: Vec<u8> = (0u8..32).collect();
        let mut rx = StreamReceiver(io::Cursor::new(two_messages));
        let mut buf = [0u8; 16];

        assert_eq!(rx.recv(&mut buf).unwrap(), RecvOutcome::Msg);
        assert_eq!(&buf[..4], &[0, 1, 2, 3]);
        assert_eq!(rx.recv(&mut buf).unwrap(), RecvOutcome::Msg);
        assert_eq!(&buf[..4], &[16, 17, 18, 19]);
        assert_eq!(rx.recv(&mut buf).unwrap(), RecvOutcome::Closed);
    }

    #[test]
    fn close_mid_message_is_an_error() {
        let short: Vec<u8> = vec![9; 10];
        let mut rx = StreamReceiver(io::Cursor::new(short));
        let mut buf = [0u8; 16];
        assert_eq!(
            rx.recv(&mut buf).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
    }
}
