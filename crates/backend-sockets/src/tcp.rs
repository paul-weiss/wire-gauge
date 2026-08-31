//! Raw TCP over loopback. `TCP_NODELAY` on both sides — with 64–256-byte
//! messages, Nagle would batch sends and measure the algorithm, not the wire.

use std::io;
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use harness::transport::{Receiver, Sender};

use crate::stream::{echo_loop, StreamReceiver, StreamSender};

const DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

pub fn echo(bind: &str, msg_size: usize) -> io::Result<()> {
    let listener = TcpListener::bind(bind)?;
    crate::announce_ready(listener.local_addr()?);
    let (sock, _peer) = listener.accept()?;
    sock.set_nodelay(true)?;
    echo_loop(sock, msg_size)
}

pub fn connect(addr: &str, _msg_size: usize) -> io::Result<(Box<dyn Sender>, Box<dyn Receiver>)> {
    let tx = TcpStream::connect(addr)?;
    tx.set_nodelay(true)?;
    let rx = tx.try_clone()?;
    rx.set_read_timeout(Some(DRAIN_TIMEOUT))?;
    Ok((Box::new(StreamSender(tx)), Box::new(StreamReceiver(rx))))
}
