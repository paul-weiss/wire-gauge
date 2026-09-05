#!/usr/bin/env bash
# Creates the wire-gauge-bench IAM user in the hobby account with the
# least-privilege policy next to this script, then writes its access key
# straight into ~/.aws (mode 600). The key is never printed.
#
# Run once, with the admin profile:   ADMIN=riverofnews ./create-bench-user.sh
set -euo pipefail
ADMIN="${ADMIN:-riverofnews}"
USER=wire-gauge-bench
POLICY=wire-gauge-bench
ACCT=$(aws sts get-caller-identity --profile "$ADMIN" --query Account --output text)
HERE="$(cd "$(dirname "$0")" && pwd)"

# 1. Managed policy from the JSON (idempotent: create or new version).
if aws iam get-policy --profile "$ADMIN" --policy-arn "arn:aws:iam::$ACCT:policy/$POLICY" >/dev/null 2>&1; then
  aws iam create-policy-version --profile "$ADMIN" \
    --policy-arn "arn:aws:iam::$ACCT:policy/$POLICY" \
    --policy-document "file://$HERE/wire-gauge-bench-policy.json" --set-as-default >/dev/null
  echo "policy $POLICY: new version set as default"
else
  aws iam create-policy --profile "$ADMIN" --policy-name "$POLICY" \
    --policy-document "file://$HERE/wire-gauge-bench-policy.json" \
    --description "wire-gauge M6: ephemeral EC2 pair in us-east-1, tag-scoped" \
    --tags Key=Project,Value=wire-gauge >/dev/null
  echo "policy $POLICY: created"
fi

# 2. The user, tagged like everything else in the account.
aws iam get-user --profile "$ADMIN" --user-name "$USER" >/dev/null 2>&1 \
  || aws iam create-user --profile "$ADMIN" --user-name "$USER" --tags Key=Project,Value=wire-gauge >/dev/null
aws iam attach-user-policy --profile "$ADMIN" --user-name "$USER" \
  --policy-arn "arn:aws:iam::$ACCT:policy/$POLICY"
echo "user $USER: exists, policy attached"

# 3. One access key, written to disk only. Refuses if a key already exists.
if [ "$(aws iam list-access-keys --profile "$ADMIN" --user-name "$USER" --query 'length(AccessKeyMetadata)' --output text)" != "0" ]; then
  echo "user $USER already has an access key; not creating another" >&2
  exit 1
fi
KEYFILE="$HOME/.aws/wire-gauge-keys.json"
umask 077
aws iam create-access-key --profile "$ADMIN" --user-name "$USER" --output json > "$KEYFILE"
AK=$(python3 -c "import json;print(json.load(open('$KEYFILE'))['AccessKey']['AccessKeyId'])")
SK=$(python3 -c "import json;print(json.load(open('$KEYFILE'))['AccessKey']['SecretAccessKey'])")
printf '\n[wire-gauge-bench]\naws_access_key_id = %s\naws_secret_access_key = %s\n' "$AK" "$SK" >> "$HOME/.aws/credentials"
printf '\n[profile wire-gauge-bench]\nregion = us-east-1\noutput = json\n' >> "$HOME/.aws/config"
echo "access key written to $KEYFILE and profile [wire-gauge-bench] added; key id ends ...${AK: -4}"

# 4. Prove it works and that it is fenced.
aws sts get-caller-identity --profile wire-gauge-bench --output text
aws ec2 describe-instances --profile wire-gauge-bench --region us-east-1 --query 'length(Reservations)' --output text >/dev/null && echo "describe: ok"
if aws ec2 describe-instances --profile wire-gauge-bench --region us-west-2 >/dev/null 2>&1; then echo "note: describe is allowed in every region by design"; fi
echo "done. Move $KEYFILE into the password manager when convenient; the credentials file already has what the CLI needs."
