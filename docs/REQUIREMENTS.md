# wire-gauge — Requirements

**What's the gauge of this wire?** A benchmark harness comparing ways to move
messages between processes — same-host IPC and networked messaging systems —
under trading-system-shaped workloads.

Agreed with Paul 2026-08-31. This file is the resume point: the decisions here
are settled — don't rediscover them. When a decision changes, change it here in
the same session.

---

## What it is, and is not

- **A benchmark and a report — not a production library or system.** Paul's
  call, 2026-08-31, and the reason it lives in `hobby/` rather than
  `~/src/trading/`. Nothing here ships, takes signups, or deploys; it sits
  outside the News River standard by design, in the same class as `primes`:
  research, picked up when it sounds fun.
- **Public sources only.** The Atlas firewall applies in full. If Atlas has
  solved any of this, we don't look — every design and every number here is
  derived from public documentation, papers, and our own measurements.
- The output that matters is **`results/` + the final comparison report**, not
  the harness code. The harness exists to make the numbers honest.

## The question

"Which is fastest?" is the wrong question. A trading system has four distinct
messaging jobs with different requirements, and the candidates live in
different classes. The deliverable is a map of *which class wins which job, by
how much, and what the tails look like*:

| Job | Shape | What matters | Natural home |
|---|---|---|---|
| Market data fan-out | 1→N, high rate | latency + throughput; loss tolerable with gap-fill | multicast/UDP, shared memory |
| Order path (strategy→risk→gateway) | 1→1 chain | p99.9 latency, zero loss | IPC / point-to-point |
| Journal / audit / replay | append + replay | durability, replay from offset | Kafka-class log |
| Control plane / ops | request-reply, low volume | convenience | NATS-class |

Two axes — same-host IPC and network protocols — with Aeron deliberately
spanning both, which makes it the hinge of the comparison.

## Candidates — round 1

**Same-host IPC:**

| Candidate | Why it's in |
|---|---|
| Custom shared-memory SPSC ring | The floor. Seqlock + cache-line padding, ~100–500ns class. Everything else is measured against this. |
| iceoryx2 | Rust-native zero-copy shared-memory pub/sub; the strongest "don't hand-roll it" candidate. Supports macOS. |
| Aeron IPC | Same API as Aeron-over-UDP, shared-memory log buffers. |
| Unix domain sockets | The practical default everyone actually uses. |
| Raw TCP (`std::net`), loopback | Reference point; no framework, just the kernel. |
| Raw UDP unicast (`std::net`), loopback | Added 2026-08-31 (Paul). The floor of the network axis: shows exactly what Aeron's reliability layer costs, since Aeron rides on UDP. Unreliable by design — the harness records drops rather than retransmitting. |

**Network / messaging systems:**

| Candidate | Why it's in |
|---|---|
| Aeron UDP | The trading-industry reference for low-latency messaging. |
| ZeroMQ | Brokerless sockets-with-patterns; the "just sockets, done well" reference. Historical HFT usage. |
| NATS core | ~50µs-class brokered fire-and-forget bus. |
| NATS JetStream | Counted as a *separate* candidate: Raft + fsync puts it in a different latency class entirely. |
| Redis Streams | The "is good-enough good enough?" candidate. Single-threaded broker, consumer groups, weaker persistence (AOF/RDB). |
| Kafka | The durability/replay standard. |

**Parked for round 2:** raw UDP multicast with MoldUDP64-style gap-fill (how
exchanges actually ship market data — building a toy one teaches feed handlers
better than any broker benchmark); Iggy (Rust-native Kafka-alike).

**Considered and cut:** RabbitMQ (wrong latency class; adds nothing over NATS
here), Chronicle Queue (JVM-only), Pulsar (heavier Kafka), MQTT (wrong domain).

**Platform wrinkles already known:**

- POSIX message queues don't exist on macOS — that classic IPC option is off
  the table.
- Redpanda has no native macOS build, and **a broker inside Docker on the Mac
  puts a VM in the data path and invalidates the latency numbers**. On the Mac,
  Kafka means JVM Kafka run natively (brew). Redpanda could join on `primes`
  later as a Kafka-protocol comparison, but round 1 keeps one Kafka.
- Aeron is the **riskiest dependency**: the media driver is Java or C (C is
  fine), but the Rust client story is FFI bindings (`rusteron`) rather than a
  first-class client. It gets its own milestone so its problems can't stall the
  rest.

## Methodology — the actual project

Benchmark harnesses are easy; honest ones are not. These rules are the project:

1. **No coordinated omission.** Load is generated on a fixed schedule and
   latency is measured from the *intended* send time, not the actual one
   (HdrHistogram, the Gil Tene discipline). Without this every brokered system
   looks 10x better than it is at p99.
2. **Tails, not medians.** Report p50 / p99 / p99.9 / p99.99 and
   throughput-vs-latency curves, stepping offered load until saturation. In
   trading, the p99.9 *is* the number.
3. **Trading-shaped workloads.** Small messages (64–256B). Three patterns:
   1→1 RTT (order path), 1→N fan-out at N=1/4/16 (market data), N→1
   (gateway in). Plus a **burst test** — quiet line, then a market-open-style
   burst — which exposes allocation, page-fault, and warm-up behavior that
   steady-state hides.
4. **One-way latency across processes** is valid same-host because the
   monotonic clock is system-wide (`CLOCK_MONOTONIC_RAW` on Linux,
   `mach_continuous_time` on macOS). Cross-host, RTT needs no clock sync at
   all (send and measure on one host); one-way needs PTP and stays parked.
   The cross-host campaign is scoped in "Rigs" below (M6).
5. **Fairness rules:** identical payloads; identical allocation discipline; at
   most two configs per system (default + one documented tuning); never one
   system's client against another system's server; brokers run natively,
   never in Docker on the Mac.
6. **Two platforms, two roles.** The Mac is the dev loop and gives relative
   ordering only — macOS has no core isolation, thread affinity is a hint on
   Apple Silicon, no busy-poll tuning. **`primes` is the canonical rig**:
   i9-13980HX (24C/32T hybrid), 62 GB RAM, Linux 6.14, `performance` governor
   already set (verified 2026-08-31). Canonical runs pin to P-cores — the
   hybrid P/E layout means unpinned numbers are bimodal garbage.
7. **Results are data, checked in.** Runs emit JSONL with machine + config +
   version metadata into `results/`; the report is generated from that, never
   hand-typed. A number that can't be traced to a run doesn't go in the report.

## Hypotheses to test

Rough latency classes from public data. The benchmark exists to confirm or
refute the *ordering* and — more importantly — to characterize the tail
behavior these single numbers hide:

| Transport | Expected class (same-host) |
|---|---|
| shm ring (custom) | 0.1–0.5 µs |
| iceoryx2 / Aeron IPC | 0.5–2 µs |
| raw UDP loopback | 3–10 µs |
| Aeron UDP | 5–15 µs |
| Unix domain socket | 5–20 µs |
| ZeroMQ | 15–30 µs |
| NATS core | 30–100 µs |
| Redis Streams | 50–200 µs |
| NATS JetStream | 0.5–2 ms |
| Kafka (acks=all) | 2–10 ms |

## Architecture

Rust workspace, matching the house stack:

- `crates/harness` — the transport traits, scenario definitions, CO-free load
  generator, HDR histogram recording, JSONL output. **No backend code here.**
- One crate per backend (`crates/backend-*`), gated behind cargo features so
  heavy dependencies (rdkafka, zmq, Aeron FFI) stay out of the default build.
  Exception, decided in M1: the three std-only raw-socket backends share
  `crates/backend-sockets` — the per-backend rule exists to quarantine heavy
  dependencies, and they have none.
- A `runner` binary that takes (backend, scenario, config) and emits one JSONL
  record per run into `results/`.
- Report generation from `results/` comes in M5; format decided then.

The `Transport` trait is designed in M1 against the two simplest backends —
not before. Designing it in the abstract is how harnesses grow warts.

**Blocking, not async, on the hot path.** The raw-socket baselines (UDS, TCP,
UDP) use blocking `std::net`/`nix` calls — a tokio runtime adds scheduler and
wakeup latency that would contaminate the very floor the baselines exist to
establish. Async clients appear only where a backend's official client is
async (e.g. `async-nats`), and that gets said in the report; a tokio variant
of a raw socket counts as the one "documented second config" if it's ever
worth measuring.

## Rigs — the machines this project needs

Scoped 2026-08-31 on Paul's ask ("need networked machines"). Bottom line:
**nothing to buy** — the same-host axis is covered by hardware that exists,
and the cross-host axis rents two EC2 instances by the hour when its
milestone arrives.

**Same-host (rounds 1–2, all current milestones):**

- **Mac** — dev loop; relative ordering only. Both toolchains kept on
  current stable (1.98.0 as of 2026-08-31, Paul's standing instruction).
- **primes** — the canonical rig (i9-13980HX, 62 GB, Linux 6.14,
  performance governor; rustup installed 2026-08-31). Runs pin with
  `taskset -c 2,4,6,12`: four distinct P-cores, HT siblings left idle,
  cpu0 avoided because it catches kernel housekeeping. Repo is rsync'd to
  `primes:wire-gauge/` (excluding `target/` and `.git/`); results scp'd
  back into `results/` and committed.

**Cross-host (M6, after the same-host report):** where Aeron, NATS and
Kafka actually live in production — a real NIC, interrupt path, and kernel
network stack on both ends. The harness needs no changes beyond running the
echo peer on the second host and passing its address instead of spawning.

- **Home LAN (free shakeout):** Mac ↔ primes proves the cross-host *mode*
  works, but is not canonical: Wi-Fi jitter is milliseconds, so wired-only,
  and the Mac end reintroduces the macOS scheduler tail.
- **AWS (canonical):** 2× EC2 — not Lightsail; this needs placement control
  and kernel tuning — in the **same AZ inside a cluster placement group**,
  which is what gets the ~40–60µs RTT floor instead of cross-AZ ~500µs+.
  c7i.xlarge on-demand ≈ $0.18/hr each, so a full evening campaign runs
  **under $5**; c6in only if a throughput scenario proves PPS-bound. Ubuntu
  LTS, same rustup + taskset discipline, irqbalance off.
- **Ephemeral by design:** an `infra/` script (aws cli) creates the pair,
  runs the campaign, terminates everything — nothing is left running, ever.
  Everything tagged `Project=wire-gauge`, which plugs into the cost
  allocation tags item already on Paul's list.
- **Account:** the hobby account `106103710708`, us-east-1, per the account
  pattern. The existing IAM users are service-scoped deploy users, so M6
  needs a small EC2-capable IAM identity (`wire-gauge-bench`) — Paul's
  call when the time comes, listed in TASKS.md.
- **Broker topology decision, deferred to M6:** brokered systems cross-host
  have three roles (publisher, broker, subscriber). Start 2-box with the
  broker co-located with the echo side; 3-box only if results demand it.
- **Multicast caveat for round 2:** a plain VPC does not carry multicast —
  that needs Transit Gateway multicast domains. The MoldUDP64 work stays
  same-host/LAN unless it earns the TGW complexity.

## Milestones

- **M0 — this document + workspace skeleton.** Done 2026-08-31.
- **M1 — methodology proven. Done 2026-08-31**, and it grew raw UDP since
  that was nearly free once it joined round 1. Harness core (transport
  traits, scheduled CO-free load gen, HDR latency + send-lag histograms,
  JSONL records with machine metadata) + three backends: UDS, TCP loopback,
  UDP unicast. The runner spawns itself as the echo peer, so every run is
  genuinely cross-process. `scripts/smoke.sh` proves all three still measure.
  Verified on the Mac: UDS p50 ≈ 28µs < UDP ≈ 38µs < TCP ≈ 42µs at 5k/s,
  zero drops, and the generator holds schedule at 100k/s (send-lag p99
  3.3µs). The send-lag histogram is the run-validity check: if it grows, the
  generator — not the transport — is the bottleneck at that rate.
- **M2 — the IPC axis. Done 2026-08-31.** Custom shm ring (SPSC LMAX-style
  ring over file-backed shared memory, backpressure not overwrite) and
  iceoryx2 0.9.3 (plain `ipc` flavor — not `ipc_threadsafe`, whose locks
  the harness doesn't need; dynamic `[u8]` payloads, 4096-sample subscriber
  buffer, busy-poll receive; its single-threaded Rc-based ports forced one
  harness change — `Sender` dropped its `Send` bound, and the iceoryx2
  receiver constructs its port lazily inside the receive thread).
  Primes, pinned, 128B: **shm p50 0.35µs / p99 0.44µs; iceoryx2 p50
  0.81–0.84µs / p99 0.94–1.09µs** — both in their hypothesized bands, zero
  drops, raw ring ≈ 2.3x faster than the framework.
  Two methodology lessons, both caught by the numbers: (1) checking
  `Instant::now()` every poll iteration adds visible p99 jitter — spin
  freely, stride the clock checks; (2) **first-touch page faults**: at
  5k msgs/s a 60k-message run covers the 64K-slot ring exactly once, and
  every ~21st message paid a soft fault (p99 7.5µs); pre-faulting the
  mapping at open dropped that to 0.44µs. The full campaign lives in
  `results/primes-20260831.jsonl`: shm 0.35µs < iceoryx2 0.8µs < uds
  3.7–4.2µs < udp 5.0–6.4µs < tcp 5.9–7.3µs at p50.
  Also proven en route: the macOS shm p999 of 1.15ms fell to 8.7µs once
  pinned on primes — the platform-honesty rule made measurable.
- **M3 — the brokered set. Done 2026-08-31.** NATS core, JetStream, Redis
  Streams, Kafka, plus broker lifecycle in the runner (started before the
  echo child on non-default ports, TCP-probed, killed and swept after —
  natively on both machines; primes runs user-space installs, no sudo).
  Primes, pinned to P-cores 0–15 (wider than the 4-core IPC pin so the
  JVM broker isn't starved), 128B, zero drops everywhere:
  **redis 24.7µs < nats 39.8µs < jetstream 55.1µs < kafka 114.4µs** at
  p50/5k msgs/s. Findings worth keeping:
  - **Kafka at 50k msgs/s collapses to p50 241ms** — every message
    delivered, none dropped, but a single-partition `acks=all`
    per-message pipeline can't sustain the rate, so the backlog's sojourn
    time is the latency. The CO-honest schedule is what makes this
    visible; send lag stays microseconds because enqueue is async.
  - JetStream and Kafka beat their hypothesized bands by an order of
    magnitude at low rate (55µs vs 0.5–2ms; 114µs vs 2–10ms): one node,
    no replication, OS-async fsync. The public numbers the hypotheses
    came from describe clusters. Single-box persistence is cheap;
    replication is what costs — a thing M6 can measure.
  - Redis beats NATS core here: one XADD round trip vs two brokered hops
    plus the async-client `block_on`+flush boundary.
  - Gotchas burned down: async-nats Subscriber::drop panics off-runtime;
    Kafka 4 won't auto-create topics for consumers (admin-create + wait
    for real partition assignment before the schedule starts); single-node
    KRaft needs `offsets.topic.replication.factor=1` or the group
    coordinator silently never exists; rdkafka's cmake build hard-fails
    without curl headers — Linux uses librdkafka's mklove configure,
    which probes and disables what's missing.
  `scripts/table.py` prints the full comparison from `results/`.
- **M4 — Aeron. Done 2026-08-31.** rusteron 0.2.5 (FFI over vendored
  Aeron C), statically linked after the predicted FFI papercuts arrived on
  schedule: the default build dynamically links a dylib with no rpath
  (fixed by the `static` feature); primes needed a user-space cmake ≥3.30
  (`~/opt/cmake`, Aeron's CMakeLists demands it) and a user-space static
  libuuid (`~/opt/uuid`, built from util-linux; found via
  `PKG_CONFIG_PATH`) — build there with
  `CMAKE=~/opt/cmake/bin/cmake PKG_CONFIG_PATH=~/opt/uuid/lib/pkgconfig`.
  Media driver embedded in the echo child, SHARED threading + spin idle
  (Aeron's low-latency single-core profile — the one documented tuning).
  Primes, pinned (2,4,6,12), 128B, zero drops:
  **aeron-ipc p50 0.47–0.53µs** (between the raw ring and iceoryx2;
  hypothesis band hit), **aeron-udp p50 4.2–5.0µs — as fast as raw UDP
  sockets.** That second number is a finding: Aeron's reliability layer
  costs ~nothing at the median because the spinning driver owns the
  sockets and hands messages to the client over shared memory, amortizing
  the syscall path the raw-socket backend pays per message. The tails are
  where Aeron pays: p99 7–8µs on IPC vs iceoryx2's 0.94µs, with four hot
  threads (parent 2, echo 1, driver 1) exactly filling the 4-core pin.
- **M5 — the report. Done 2026-08-31.** `scripts/report.py` generates
  `report/wire-gauge-report.html` from `results/*.jsonl` — never
  hand-typed: charts, findings numbers, and the full table all come from
  the newest committed record per configuration. Self-contained
  theme-aware HTML (light + dark), two log-axis SVG charts (the
  p50→p99→p99.9 ladder classed as shared-memory / kernel-sockets /
  brokered, and a low-vs-high-rate dumbbell that makes the kafka
  saturation collapse visible), the findings, the jobs-to-transports map,
  and every run as a table. Regenerate after any campaign:
  `python3 scripts/report.py`. Published as a private artifact:
  https://claude.ai/code/artifact/4b013200-0182-4dc5-8bd0-f249a7454034
  (republish after regenerating).
- **M6 — cross-host on AWS.** Two EC2 boxes in a cluster placement group,
  the network-axis candidates re-measured over a real wire. See "Rigs".
  Needs Paul: the `wire-gauge-bench` IAM identity.

## Repo conventions

- **Public on GitHub since 2026-09-05:** `paul-weiss/wire-gauge`. GitHub is
  the backup; nothing here needs the rsync INCLUDE list.
- Trunk is `main`. Commit straight to `main` with prose commit messages
  (Paul, 2026-09-05: "check everything into main for now").
- `results/` is versioned deliberately — runs are the product.
