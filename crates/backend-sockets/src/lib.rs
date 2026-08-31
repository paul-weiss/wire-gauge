//! The raw-socket backends: Unix domain sockets, TCP loopback, UDP unicast.
//!
//! All three are `std::net`/`std::os::unix::net` blocking sockets — no
//! framework, no runtime — because these are the floors the fancier
//! transports get measured against (REQUIREMENTS.md, "Blocking, not async,
//! on the hot path"). They share one crate, unlike heavier backends, because
//! the one-crate-per-backend rule exists to quarantine heavy dependencies
//! and these have none.
//!
//! Every backend exposes the same pair of roles:
//! - `echo(bind, msg_size)` — the child-process peer. Binds, prints
//!   `READY <addr>` on stdout for the orchestrator to read, then echoes
//!   every message back verbatim until the peer goes away.
//! - `connect(addr, msg_size)` — the measuring side's split connection.

use std::io;

use harness::transport::{Receiver, Sender};

mod stream;
pub mod tcp;
pub mod udp;
pub mod uds;

pub const BACKENDS: &[&str] = &["uds", "tcp", "udp"];

/// The bind address the orchestrator hands the echo child; `:0` / a fresh
/// temp path so parallel runs can't collide. The child prints the concrete
/// address it actually bound.
pub fn default_bind(backend: &str) -> io::Result<String> {
    match backend {
        "tcp" | "udp" => Ok("127.0.0.1:0".to_string()),
        "uds" => Ok(std::env::temp_dir()
            .join(format!("wire-gauge-{}.sock", std::process::id()))
            .to_string_lossy()
            .into_owned()),
        other => Err(unknown_backend(other)),
    }
}

pub fn echo(backend: &str, bind: &str, msg_size: usize) -> io::Result<()> {
    match backend {
        "tcp" => tcp::echo(bind, msg_size),
        "udp" => udp::echo(bind, msg_size),
        "uds" => uds::echo(bind, msg_size),
        other => Err(unknown_backend(other)),
    }
}

pub fn connect(
    backend: &str,
    addr: &str,
    msg_size: usize,
) -> io::Result<(Box<dyn Sender>, Box<dyn Receiver>)> {
    match backend {
        "tcp" => tcp::connect(addr, msg_size),
        "udp" => udp::connect(addr, msg_size),
        "uds" => uds::connect(addr, msg_size),
        other => Err(unknown_backend(other)),
    }
}

fn unknown_backend(name: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("unknown backend '{name}' (have: {})", BACKENDS.join(", ")),
    )
}

/// Print the line the orchestrator waits for. Flushes: the child's stdout is
/// a pipe, so line buffering can't be relied on.
fn announce_ready(addr: impl std::fmt::Display) {
    use std::io::Write;
    let mut out = io::stdout();
    writeln!(out, "READY {addr}").expect("announce READY");
    out.flush().expect("flush READY");
}
