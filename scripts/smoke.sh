#!/usr/bin/env bash
# Quick end-to-end proof that every wired backend still measures: short rtt
# run each, one summary line per backend. Not a benchmark — durations are too
# short and nothing is pinned; use real runs on primes for numbers.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo build --release --quiet
for b in shm iceoryx2 aeron-ipc aeron-udp uds tcp udp nats jetstream redis kafka; do
  ./target/release/wire-gauge rtt "$b" --rate 5000 --size 128 --duration 2 --warmup 1 2>/dev/null |
    python3 -c "
import json, sys
r = json.load(sys.stdin); res = r['results']; l = res['latency_ns']
print(f\"{r['backend']:>4}: recv={res['received']}/{res['sent']} dropped={res['dropped']} \"
      f\"p50={l['p50']/1000:.1f}us p99={l['p99']/1000:.1f}us p999={l['p999']/1000:.1f}us\")
"
done
