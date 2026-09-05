#!/usr/bin/env bash
# Provision the rig from state.json: toolchain + brokers on every host,
# build the runner once on a, ship the binary to the echo hosts.
source "$(dirname "$0")/lib.sh"
HOSTS=$(python3 -c "import json; print(' '.join(sorted(json.load(open('$STATE'))['hosts'])))")
for k in $HOSTS; do
  ip=$(state "['hosts']['$k']['pub']"); role=echo; [ "$k" = a ] && role=builder
  log "setup $k ($ip) as $role"
  ssh_ "$SSH_USER@$ip" "bash -s $role" < "$HERE/remote-setup.sh" &
done
wait
A=$(state "['hosts']['a']['pub']")
log "rsync repo to a"
rsync -az --delete -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR" \
  --exclude target --exclude .git --exclude results --exclude report --exclude 'infra/aws/state.json' "$REPO/" "$SSH_USER@$A:wire-gauge/"
log "cargo build --release on a (Aeron's C build is the slow part)"
ssh_ "$SSH_USER@$A" 'cd wire-gauge && CMAKE=$HOME/opt/cmake/bin/cmake PATH=$HOME/opt/cmake/bin:$HOME/.cargo/bin:$PATH cargo build --release 2>&1 | tail -3 && cp target/release/wire-gauge ~/wire-gauge-bin'
for k in $HOSTS; do
  [ "$k" = a ] && continue
  ip=$(state "['hosts']['$k']['pub']")
  log "ship binary to $k"
  scp_ -3 "$SSH_USER@$A:wire-gauge-bin" "$SSH_USER@$ip:wire-gauge-bin"
  ssh_ "$SSH_USER@$ip" 'chmod +x wire-gauge-bin && ./wire-gauge-bin --help | head -1'
done
log "provisioned. ./campaign.sh next."
