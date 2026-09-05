#!/usr/bin/env bash
# Create the M6 rig: key pair, security group, cluster placement group, and
# three c7i.2xlarge Ubuntu hosts — a (client) and b (echo) together in the
# placement group in AZ_A, c (echo) alone in AZ_B for the cross-AZ row.
# Idempotent where AWS allows it. Writes state.json. Costs start here.
#
#   ./up.sh            three hosts
#   ./up.sh --no-cross  a and b only
source "$(dirname "$0")/lib.sh"
CROSS=1; [ "${1:-}" = "--no-cross" ] && CROSS=0

[ -f "$SSH_KEY" ] || { echo "no ssh key at $SSH_KEY" >&2; exit 1; }
ssh-keygen -y -P "" -f "$SSH_KEY" >/dev/null 2>&1 || { echo "$SSH_KEY has a passphrase; add it to ssh-agent or point WG_SSH_KEY at a key without one" >&2; exit 1; }

log "key pair $NAME"
aws ec2 import-key-pair --key-name "$NAME" --public-key-material "fileb://$SSH_KEY.pub" \
  --tag-specifications "ResourceType=key-pair,Tags=[{Key=Project,Value=$PROJECT}]" >/dev/null 2>&1 || true

VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
log "security group $NAME in $VPC"
SG=$(aws ec2 describe-security-groups --filters Name=group-name,Values="$NAME" Name=vpc-id,Values="$VPC" --query 'SecurityGroups[0].GroupId' --output text)
if [ "$SG" = "None" ]; then
  SG=$(aws ec2 create-security-group --group-name "$NAME" --description "wire-gauge M6 bench pair" --vpc-id "$VPC" \
    --tag-specifications "ResourceType=security-group,Tags=[{Key=Project,Value=$PROJECT}]" --query GroupId --output text)
  MYIP=$(curl -s https://checkip.amazonaws.com)
  aws ec2 authorize-security-group-ingress --group-id "$SG" --protocol tcp --port 22 --cidr "$MYIP/32" >/dev/null
  # Everything between the hosts: the bench ports are all inside the group.
  aws ec2 authorize-security-group-ingress --group-id "$SG" --protocol -1 --source-group "$SG" >/dev/null
fi

log "placement group $PG_NAME (cluster)"
aws ec2 create-placement-group --group-name "$PG_NAME" --strategy cluster \
  --tag-specifications "ResourceType=placement-group,Tags=[{Key=Project,Value=$PROJECT}]" >/dev/null 2>&1 || true

subnet_for() { aws ec2 describe-subnets --filters Name=default-for-az,Values=true Name=availability-zone,Values="$1" --query 'Subnets[0].SubnetId' --output text; }
launch() { # name role az placement(0/1)
  local name=$1 role=$2 az=$3 pg=$4 extra=()
  [ "$pg" = 1 ] && extra=(--placement "GroupName=$PG_NAME,AvailabilityZone=$az") || extra=(--placement "AvailabilityZone=$az")
  aws ec2 run-instances --image-id "$AMI" --instance-type "$INSTANCE_TYPE" --key-name "$NAME" \
    --security-group-ids "$SG" --subnet-id "$(subnet_for "$az")" "${extra[@]}" \
    --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=30,VolumeType=gp3,DeleteOnTermination=true}' \
    --instance-initiated-shutdown-behavior terminate \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Project,Value=$PROJECT},{Key=Name,Value=$name},{Key=Role,Value=$role}]" "ResourceType=volume,Tags=[{Key=Project,Value=$PROJECT}]" \
    --query 'Instances[0].InstanceId' --output text
}

log "launching a (client) + b (echo) in $AZ_A / $PG_NAME"
A=$(launch wire-gauge-a client "$AZ_A" 1)
B=$(launch wire-gauge-b echo-same-az "$AZ_A" 1)
IDS=("$A" "$B")
if [ "$CROSS" = 1 ]; then
  log "launching c (echo, cross-AZ) in $AZ_B"
  C=$(launch wire-gauge-c echo-cross-az "$AZ_B" 0); IDS+=("$C")
fi
log "waiting for running: ${IDS[*]}"
aws ec2 wait instance-running --instance-ids "${IDS[@]}"

aws ec2 describe-instances --instance-ids "${IDS[@]}" \
  --query 'Reservations[].Instances[].{id:InstanceId,name:Tags[?Key==`Name`]|[0].Value,role:Tags[?Key==`Role`]|[0].Value,az:Placement.AvailabilityZone,pub:PublicIpAddress,priv:PrivateIpAddress,type:InstanceType}' \
  --output json | python3 -c "
import json,sys
hosts={h['name'].split('-')[-1]:h for h in json.load(sys.stdin)}
json.dump({'sg':'$SG','hosts':hosts}, open('$STATE','w'), indent=2)
for k,h in sorted(hosts.items()): print(f\"  {k}: {h['id']} {h['az']} pub={h['pub']} priv={h['priv']} ({h['role']})\")
"
log "waiting for ssh"
for k in a b ${C:+c}; do
  ip=$(state "['hosts']['$k']['pub']")
  for i in $(seq 1 60); do ssh_ "$SSH_USER@$ip" true 2>/dev/null && break; sleep 5; done
  ssh_ "$SSH_USER@$ip" true || { echo "ssh to $k ($ip) never came up" >&2; exit 1; }
done
log "rig is up. state in $STATE. Next: ./provision.sh, then ./campaign.sh (which runs ./down.sh unless --keep)."
