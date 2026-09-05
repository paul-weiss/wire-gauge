# Shared settings for the M6 cross-host campaign. Sourced, not run.
# Everything is tagged Project=wire-gauge and lives in the hobby account's
# default VPC in us-east-1, created and destroyed per campaign.
set -euo pipefail

PROFILE="${WG_PROFILE:-wire-gauge-bench}"
REGION="${WG_REGION:-us-east-1}"
PROJECT=wire-gauge
NAME=wire-gauge-bench                       # key pair, security group
PG_NAME=wire-gauge-cluster                  # placement group (cluster)
INSTANCE_TYPE="${WG_INSTANCE_TYPE:-c7i.2xlarge}"
# Ubuntu 24.04 LTS amd64, us-east-1, resolved 2026-09-05 from the canonical
# SSM parameter. Override with WG_AMI if it has aged out.
AMI="${WG_AMI:-ami-025d99823a4caad37}"
AZ_A="${WG_AZ_A:-us-east-1a}"               # client + same-AZ echo, in the placement group
AZ_B="${WG_AZ_B:-us-east-1b}"               # cross-AZ echo
SSH_KEY="${WG_SSH_KEY:-$HOME/.ssh/id_ed25519}"
SSH_USER=ubuntu
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
STATE="$HERE/state.json"                    # gitignored; ids and IPs of the live pair

aws() { command aws --profile "$PROFILE" --region "$REGION" "$@"; }
ssh_() { command ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ServerAliveInterval=15 "$@"; }
scp_() { command scp -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR "$@"; }
state() { python3 -c "import json,sys; d=json.load(open('$STATE')); print(eval('d'+sys.argv[1]))" "$1"; }
log() { printf '\033[1m[%s] %s\033[0m\n' "$(date +%H:%M:%S)" "$*" >&2; }
