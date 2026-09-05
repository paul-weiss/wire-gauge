#!/usr/bin/env bash
# Destroy everything tagged Project=wire-gauge: instances first, then the
# security group, placement group, and key pair. Safe to run repeatedly.
source "$(dirname "$0")/lib.sh"
IDS=$(aws ec2 describe-instances --filters "Name=tag:Project,Values=$PROJECT" Name=instance-state-name,Values=pending,running,stopping,stopped \
  --query 'Reservations[].Instances[].InstanceId' --output text)
if [ -n "$IDS" ]; then
  log "terminating $IDS"
  aws ec2 terminate-instances --instance-ids $IDS >/dev/null
  aws ec2 wait instance-terminated --instance-ids $IDS
fi
SG=$(aws ec2 describe-security-groups --filters Name=group-name,Values="$NAME" --query 'SecurityGroups[0].GroupId' --output text)
if [ "$SG" != "None" ]; then
  log "deleting security group $SG"
  for i in $(seq 1 12); do aws ec2 delete-security-group --group-id "$SG" 2>/dev/null && break; sleep 10; done
fi
log "deleting placement group and key pair"
aws ec2 delete-placement-group --group-name "$PG_NAME" 2>/dev/null || true
aws ec2 delete-key-pair --key-name "$NAME" >/dev/null 2>&1 || true
rm -f "$STATE"
LEFT=$(aws ec2 describe-instances --filters "Name=tag:Project,Values=$PROJECT" Name=instance-state-name,Values=pending,running,stopping,stopped --query 'length(Reservations[].Instances[])' --output text)
log "down. instances still alive with the tag: $LEFT"
