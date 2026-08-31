//! Unix domain sockets, stream flavor (`SOCK_STREAM`) — the standard
//! comparison point. A `SOCK_DGRAM` variant would be a separate backend if
//! it ever earns a slot.

use std::io;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::time::Duration;

use harness::transport::{Receiver, Sender};

use crate::stream::{echo_loop, StreamReceiver, StreamSender};

const DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

pub fn echo(bind: &str, msg_size: usize) -> io::Result<()> {
    // A previous run's socket file would make bind fail with AddrInUse.
    let path = Path::new(bind);
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    let listener = UnixListener::bind(path)?;
    crate::announce_ready(bind);
    let (sock, _peer) = listener.accept()?;
    echo_loop(sock, msg_size)
}

pub fn connect(addr: &str, _msg_size: usize) -> io::Result<(Box<dyn Sender>, Box<dyn Receiver>)> {
    let tx = UnixStream::connect(addr)?;
    let rx = tx.try_clone()?;
    rx.set_read_timeout(Some(DRAIN_TIMEOUT))?;
    Ok((Box::new(StreamSender(tx)), Box::new(StreamReceiver(rx))))
}
