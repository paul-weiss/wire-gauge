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
- **M2 — the IPC axis.** Custom shm ring (**done 2026-08-31** — SPSC
  LMAX-style ring over file-backed shared memory, backpressure not
  overwrite; primes pinned: p50 0.33µs RTT, p999 2.2–8.7µs) + iceoryx2
  (open). First primes campaign ran the same day — 4 backends × 2 rates in
  `results/primes-20260831.jsonl`: shm 0.33µs < uds 3.7–4.2µs < udp
  5.0–6.4µs < tcp 5.9–7.3µs at p50, zero drops, hypothesis ordering
  confirmed; the macOS shm p999 of 1.15ms fell to 8.7µs once pinned —
  the platform-honesty rule made measurable.
- **M3 — the brokered set.** NATS core, JetStream, Redis Streams, Kafka.
- **M4 — Aeron**, IPC + UDP, quarantined because of the FFI risk.
- **M5 — the report.** Comparison doc + charts, generated from `results/`.
- **M6 — cross-host on AWS.** Two EC2 boxes in a cluster placement group,
  the network-axis candidates re-measured over a real wire. See "Rigs".
  Needs Paul: the `wire-gauge-bench` IAM identity.

## Repo conventions

- **Local-only for now** — Paul creates the GitHub repo later (public vs
  private undecided). Until it exists, this directory lives on the laptop
  alone: it is not on GitHub **and not in the backup INCLUDE list**, the same
  exposure `meeting-facts` has. Flag this whenever the work grows past
  throwaway size.
- Trunk is `main`. While the repo is local-only there are no PRs; commit to
  `main` with the usual prose commit messages. Adopt the feature-branch + PR
  convention when the GitHub repo appears.
- `results/` is versioned deliberately — runs are the product.
