# wire-gauge

**What's the gauge of this wire?** A benchmark comparing ways to move messages
between processes under trading-system-shaped workloads: same-host IPC
(a custom shared-memory ring, iceoryx2, Aeron IPC, Unix domain sockets, TCP
loopback, UDP) and brokered messaging systems (Aeron UDP, NATS core, NATS
JetStream, Redis Streams, Kafka).

Not a production library. The product is the numbers: coordinated-omission-free
latency distributions in `results/`, and a report generated from them, never
hand-typed.

## Round 1 results

Eleven transports, one machine, one methodology. Round-trip latency in
microseconds at 5,000 messages per second, 128-byte payloads, pinned to
isolated P-cores on Linux. Zero drops in every run.

| Transport | p50 | p99 | p99.9 | Class |
|---|---:|---:|---:|---|
| shm (custom SPSC ring) | 0.35 | 0.44 | 0.85 | shared memory |
| aeron-ipc | 0.53 | 8.15 | 14.81 | shared memory |
| iceoryx2 | 0.81 | 0.94 | 2.29 | shared memory |
| uds | 4.16 | 19.05 | 27.76 | kernel sockets |
| aeron-udp | 5.03 | 20.43 | 24.70 | kernel sockets |
| udp | 6.43 | 25.95 | 34.43 | kernel sockets |
| tcp | 7.30 | 24.41 | 31.86 | kernel sockets |
| redis streams | 24.66 | 73.53 | 129.79 | brokered |
| nats core | 39.81 | 100.29 | 156.16 | brokered |
| nats jetstream | 55.10 | 110.97 | 243.46 | brokered |
| kafka | 114.43 | 199.68 | 676.35 | brokered |

`python3 scripts/table.py` prints every run, including the 50,000 and
100,000 messages-per-second campaigns and the p99.99 and max columns.

### Findings worth keeping

- **Kafka at 50,000 messages per second: p50 of 241 milliseconds, zero
  drops.** Every message was delivered. A single-partition, `acks=all`,
  per-message pipeline cannot sustain the rate, so the backlog's sojourn time
  becomes the latency. A load generator that waits for the previous send
  would never see this; the scheduled, coordinated-omission-free generator is
  what makes it visible. The same rate on NATS core costs 34 microseconds.
- **Aeron UDP is as fast as raw UDP sockets at the median.** The reliability
  layer costs almost nothing at p50 because the spinning media driver owns
  the sockets and hands messages to the client over shared memory. Aeron
  pays in the tails instead: p99 of 7 to 8 microseconds on IPC against
  iceoryx2's 0.94.
- **Single-node persistence is cheap; replication is what costs.** JetStream
  and Kafka beat their hypothesized bands by an order of magnitude at low
  rate. The public numbers those hypotheses came from describe clusters.
- **A raw ring is about 2.3x faster than the framework built on the same
  idea.** shm 0.35 microseconds against iceoryx2 0.81. The framework buys
  you discovery, lifetimes, and safety; this is the price.
- **Two methodology lessons the numbers caught:** checking the clock on every
  poll iteration adds visible p99 jitter (spin freely, stride the clock
  checks), and first-touch page faults on a 64K-slot ring cost every 21st
  message 7.5 microseconds at p99 until the mapping was pre-faulted.
- **Platform honesty is measurable.** The shared-memory ring's p99.9 on
  macOS was 1.15 milliseconds. Pinned on Linux it was 8.7 microseconds.
  Nothing about the code changed.

## Methodology

`docs/REQUIREMENTS.md` is the plan of record: the candidate set, the
hypotheses, the rules, and the milestones. The rules that matter most:

- **Scheduled load, not closed-loop.** Sends happen on a fixed schedule; a
  slow transport does not slow the generator. Latency is measured from the
  scheduled send time, so coordinated omission is not silently corrected
  away. A separate send-lag histogram is the run-validity check: if it
  grows, the generator is the bottleneck, and the run is discarded.
- **Tails over medians.** Every record carries an HDR histogram; the report
  shows the p50 to p99 to p99.9 ladder, not a bar chart of averages.
- **Genuinely cross-process.** The runner spawns itself as the echo peer.
  Brokers run natively, started before the echo child on non-default ports,
  probed, and killed and swept after each run. No brokers in Docker.
- **Fair pinning.** IPC runs pin to four distinct P-cores with hyperthread
  siblings idle and cpu0 avoided. Brokered runs get a wider pin so the JVM
  broker is not starved.
- **The Mac is a dev loop, not a rig.** Canonical numbers come from one
  Linux machine (i9-13980HX, 62 GB, Linux 6.14, performance governor).
  Relative ordering on macOS holds; the tails do not.

## Running it

```sh
cargo build --release
./target/release/wire-gauge rtt shm --rate 5000 --size 128 --duration 10
./target/release/wire-gauge rtt kafka --rate 50000 --out results/local.jsonl
scripts/smoke.sh            # short run of every backend, one line each
python3 scripts/table.py    # the comparison table from results/
python3 scripts/report.py   # regenerate report/wire-gauge-report.html
```

Backends: `shm`, `iceoryx2`, `aeron-ipc`, `aeron-udp`, `uds`, `tcp`, `udp`,
`nats`, `jetstream`, `redis`, `kafka`. The brokered backends expect
`nats-server`, `redis-server`, and a Kafka 4 (KRaft) install on the path;
the runner manages their lifecycle. Aeron is statically linked through
`rusteron` and needs cmake 3.30 or later and libuuid at build time.

## Layout

```
crates/harness         transport traits, scheduled load generator, HDR
                       histograms, JSONL run records with machine metadata
crates/backend-*       one crate per transport family
crates/runner          the wire-gauge binary: rtt scenario, echo peer,
                       broker lifecycle
results/               every canonical run, versioned; runs are the product
report/                generated HTML report (light and dark), never edited
scripts/               smoke.sh, table.py, report.py
docs/REQUIREMENTS.md   plan of record
```

## Status

Round 1 complete, 2026-08-31, milestones M0 through M5: methodology,
the IPC axis, the brokered set, Aeron, and the generated report.

Open:

- **M6, cross-host on AWS.** Two EC2 instances in a cluster placement group,
  the network-axis transports re-measured over a real NIC and kernel stack.
  Scoped in `docs/REQUIREMENTS.md` under Rigs.
- **ZeroMQ**, a round-1 candidate that was never built.
- **Round 2:** UDP multicast, Iggy, fan-out and burst scenarios.

## License

MIT.
