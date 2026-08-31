# wire-gauge

**What's the gauge of this wire?** A benchmark comparing ways to move messages
between processes — same-host IPC (shared memory, iceoryx2, Unix domain
sockets, TCP loopback, Aeron IPC) and networked messaging systems (Aeron UDP,
ZeroMQ, NATS core and JetStream, Redis Streams, Kafka) — under
trading-system-shaped workloads.

Not a production library. The product is the numbers: honest,
coordinated-omission-free latency distributions in `results/`, and the
comparison report generated from them.

Read `docs/REQUIREMENTS.md` first — the candidate set, methodology rules, and
milestones are settled there.

## Status

M0 (requirements + skeleton), 2026-08-31. No benchmarks exist yet.
