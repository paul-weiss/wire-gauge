//! Measurement machinery shared by every backend: the transport abstraction,
//! the scheduled (coordinated-omission-free) load generator, HDR histogram
//! recording, and the JSONL result record.
//!
//! Backend implementations never live in this crate.

pub mod clock;
pub mod report;
pub mod scenario;
pub mod transport;
