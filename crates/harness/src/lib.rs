//! Measurement machinery shared by every backend: the transport abstraction,
//! scenario definitions, the scheduled (coordinated-omission-free) load
//! generator, HDR histogram recording, and JSONL result output.
//!
//! Deliberately empty at M0. The `Transport` trait gets designed in M1
//! against the two simplest real backends (Unix domain sockets and TCP
//! loopback), not in the abstract — see docs/REQUIREMENTS.md, "Architecture".
//! Backend implementations never live in this crate.
