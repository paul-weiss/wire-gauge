#!/usr/bin/env python3
"""Print the run records in results/*.jsonl as one comparison table.

Keeps only the newest record per (topology, machine, backend, scenario, rate, size) —
re-runs supersede old numbers, which stay in the file for the audit trail.
"""
import glob
import json
import os
import sys

def main() -> None:
    root = os.path.join(os.path.dirname(__file__), "..", "results")
    paths = sys.argv[1:] or sorted(glob.glob(os.path.join(root, "*.jsonl")))
    latest = {}
    for path in paths:
        with open(path) as f:
            for line in f:
                r = json.loads(line)
                key = (
                    r.get("topology", "same-host"),
                    r["machine"]["hostname"],
                    r["backend"],
                    r["scenario"],
                    r["config"]["rate"],
                    r["config"]["msg_size"],
                )
                if key not in latest or r["unix_time_s"] >= latest[key]["unix_time_s"]:
                    latest[key] = r

    rows = sorted(
        latest.values(),
        key=lambda r: (
            r.get("topology", "same-host"),
            r["machine"]["hostname"],
            r["results"]["latency_ns"]["p50"],
            r["config"]["rate"],
        ),
    )
    us = lambda v: f"{v / 1000:,.2f}"
    hdr = f"{'topology':>12} {'machine':>8} {'backend':>9} {'rate/s':>7} {'size':>5} {'recv':>9} {'drop':>6} " \
          f"{'p50µs':>10} {'p99µs':>10} {'p999µs':>10} {'p9999µs':>10} {'maxµs':>10} {'lag99µs':>8}"
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        res, lat, lag = r["results"], r["results"]["latency_ns"], r["results"]["send_lag_ns"]
        print(
            f"{r.get('topology', 'same-host'):>12} {r['machine']['hostname'][:8]:>8} {r['backend']:>9} {r['config']['rate']:>7} "
            f"{r['config']['msg_size']:>5} {res['received']:>9} {res['dropped']:>6} "
            f"{us(lat['p50']):>10} {us(lat['p99']):>10} {us(lat['p999']):>10} "
            f"{us(lat['p9999']):>10} {us(lat['max']):>10} {us(lag['p99']):>8}"
        )

if __name__ == "__main__":
    main()
