//! Raw UDP unicast over loopback. Unreliable by design: nothing here
//! retransmits, and the harness reports drops instead of hiding them —
//! the delta against Aeron UDP is the price of a reliability layer.
//! One message per datagram, so no framing.

use std::io;
use std::net::UdpSocket;
use std::time::Duration;

use harness::transport::{Receiver, RecvOutcome, Sender};

const DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

pub fn echo(bind: &str, msg_size: usize) -> io::Result<()> {
    let sock = UdpSocket::bind(bind)?;
    crate::announce_ready(sock.local_addr()?);
    let mut buf = vec![0u8; msg_size];
    loop {
        let (n, peer) = sock.recv_from(&mut buf)?;
        sock.send_to(&buf[..n], peer)?;
    }
}

pub fn connect(addr: &str, _msg_size: usize) -> io::Result<(Box<dyn Sender>, Box<dyn Receiver>)> {
    // Any interface: the echo may be on another host (M6).
    let tx = UdpSocket::bind("0.0.0.0:0")?;
    tx.connect(addr)?;
    let rx = tx.try_clone()?;
    rx.set_read_timeout(Some(DRAIN_TIMEOUT))?;
    Ok((Box::new(UdpSender(tx)), Box::new(UdpReceiver(rx))))
}

struct UdpSender(UdpSocket);

impl Sender for UdpSender {
    fn send(&mut self, msg: &[u8]) -> io::Result<()> {
        let n = self.0.send(msg)?;
        if n != msg.len() {
            return Err(io::Error::other("short datagram send"));
        }
        Ok(())
    }
}

struct UdpReceiver(UdpSocket);

impl Receiver for UdpReceiver {
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<RecvOutcome> {
        match self.0.recv(buf) {
            Ok(n) if n == buf.len() => Ok(RecvOutcome::Msg),
            Ok(n) => Err(io::Error::other(format!(
                "datagram of {n} bytes, expected {}",
                buf.len()
            ))),
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                Ok(RecvOutcome::TimedOut)
            }
            Err(e) => Err(e),
        }
    }
}
