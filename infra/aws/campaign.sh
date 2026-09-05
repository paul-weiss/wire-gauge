#!/usr/bin/env bash
# Run the cross-host campaign: for each topology in state.json (b = same AZ,
# c = cross AZ), each network backend, each rate, start the echo on the far
# host, read its READY line, measure from a, and append the record locally.
# Then tear the rig down unless --keep.
#
#   ./campaign.sh [--keep] [--backends "tcp udp ..."] [--rates "5000 50000"] [--topologies "same-az cross-az"]
source "$(dirname "$0")/lib.sh"
KEEP=0; BACKENDS="tcp udp aeron-udp nats jetstream redis kafka"; RATES="5000 50000"; TOPOS=""
while [ $# -gt 0 ]; do case "$1" in
  --keep) KEEP=1;; --backends) BACKENDS=$2; shift;; --rates) RATES=$2; shift;; --topologies) TOPOS=$2; shift;;
  *) echo "unknown arg $1" >&2; exit 1;; esac; shift; done
[ -n "$TOPOS" ] || TOPOS="same-az$(python3 -c "import json; print(' cross-az' if 'c' in json.load(open('$STATE'))['hosts'] else '')")"

DATE=$(date +%Y%m%d); OUT="$REPO/results/aws-$DATE.jsonl"; NOTES="$REPO/results/aws-$DATE-notes.txt"
A_PUB=$(state "['hosts']['a']['pub']"); A_PRIV=$(state "['hosts']['a']['priv']")
ECHO_LOG=$(mktemp -d)

port_for() { case "$1" in tcp) echo 15001;; udp) echo 15002;; nats|jetstream) echo 14222;; redis) echo 16379;; kafka) echo 19092;; esac; }
# The bracket trick keeps pkill -f from matching the remote shell running
# this very command line (which would kill the ssh session and, under set -e,
# the campaign). Never let a sweep failure abort the run.
echo_sweep() { ssh_ "$SSH_USER@$1" 'pkill -f "wire-gauge-bin ech[o]" ; pkill -f "nats-serve[r]" ; pkill -f "redis-serve[r]" ; pkill -f "kafka.Kafk[a]" ; rm -rf /tmp/wire-gauge-* ; true' >/dev/null 2>&1 || true; }

{
  echo "wire-gauge M6 campaign $DATE, instance type $INSTANCE_TYPE"
  echo "a (client): $A_PRIV $(state "['hosts']['a']['az']")"
  for t in $TOPOS; do k=b; [ "$t" = cross-az ] && k=c; echo "$t echo host $k: $(state "['hosts']['$k']['priv']") $(state "['hosts']['$k']['az']")"; done
} | tee -a "$NOTES" >&2   # append: supplementary passes must not erase earlier floors

for topo in $TOPOS; do
  k=b; [ "$topo" = cross-az ] && k=c
  E_PUB=$(state "['hosts']['$k']['pub']"); E_PRIV=$(state "['hosts']['$k']['priv']")
  log "== $topo: a ($A_PRIV) -> $k ($E_PRIV)"
  { echo; echo "[$topo] icmp floor, a -> $k:"; ssh_ "$SSH_USER@$A_PUB" "ping -c 20 -i 0.2 -q $E_PRIV | tail -2"; } | tee -a "$NOTES" >&2
  for b in $BACKENDS; do
    for rate in $RATES; do
      echo_sweep "$E_PUB"
      case "$b" in
        aeron-udp) BIND="/tmp/wire-gauge-aeron-echo?c2s=$E_PRIV:20121&s2c=$A_PRIV:20122"; BROKER="";;
        nats|jetstream|redis|kafka) BIND="$E_PRIV:$(port_for "$b")"; BROKER="--broker";;
        *) BIND="$E_PRIV:$(port_for "$b")"; BROKER="";;
      esac
      OUTF="$ECHO_LOG/$topo-$b-$rate.out"; : > "$OUTF"
      ssh_ "$SSH_USER@$E_PUB" "taskset -c 0-3 ./wire-gauge-bin echo $b --bind '$BIND' --size 128 $BROKER" > "$OUTF" 2> "$OUTF.err" &
      SSHPID=$!
      READY=""; for i in $(seq 1 240); do READY=$(grep -m1 '^READY ' "$OUTF" | cut -d' ' -f2- || true); [ -n "$READY" ] && break; kill -0 $SSHPID 2>/dev/null || break; sleep 0.5; done
      if [ -z "$READY" ]; then log "!! $topo $b: echo never announced READY"; tail -5 "$OUTF.err" >&2; kill $SSHPID 2>/dev/null || true; echo_sweep "$E_PUB"; continue; fi
      log "$topo $b @ $rate/s  (peer $READY)"
      LINE=$(ssh_ "$SSH_USER@$A_PUB" "cd wire-gauge && taskset -c 0-3 ./target/release/wire-gauge rtt $b --peer '$READY' --topology aws-$topo --rate $rate --size 128 --duration 10 --warmup 2 2>/tmp/rtt.err; tail -2 /tmp/rtt.err >&2" | tail -1 || true)
      if [ -n "$LINE" ] && printf '%s' "$LINE" | python3 -c 'import json,sys; json.loads(sys.stdin.read())' 2>/dev/null; then
        printf '%s\n' "$LINE" >> "$OUT"
        printf '%s' "$LINE" | python3 -c "import json,sys; r=json.load(sys.stdin); l=r['results']['latency_ns']; print(f\"     recv={r['results']['received']}/{r['results']['sent']} drop={r['results']['dropped']} p50={l['p50']/1000:.1f}us p99={l['p99']/1000:.1f}us p999={l['p999']/1000:.1f}us lag99={r['results']['send_lag_ns']['p99']/1000:.1f}us\")" >&2
      else
        log "!! $topo $b @ $rate: no record"
      fi
      kill $SSHPID 2>/dev/null || true; wait $SSHPID 2>/dev/null || true
      echo_sweep "$E_PUB"
    done
  done
done
log "campaign done: $OUT ($(wc -l < "$OUT" 2>/dev/null || echo 0) records). echo logs in $ECHO_LOG"
if [ "$KEEP" = 1 ]; then log "--keep: rig left running. Remember ./down.sh"; else "$HERE/down.sh"; fi
