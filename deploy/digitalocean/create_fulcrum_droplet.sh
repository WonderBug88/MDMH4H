#!/usr/bin/env bash
set -euo pipefail

NAME="${NAME:-fulcrum-prod-nyc3}"
REGION="${REGION:-nyc3}"
SIZE="${SIZE:-s-1vcpu-2gb}"
IMAGE="${IMAGE:-ubuntu-24-04-x64}"
TAGS="${TAGS:-fulcrum,production}"
SSH_KEYS="${DO_SSH_KEY_IDS:-}"

if ! command -v doctl >/dev/null 2>&1; then
  echo "doctl is required. Install it and authenticate with a DigitalOcean token first."
  exit 1
fi

if [[ -z "${SSH_KEYS}" ]]; then
  echo "Set DO_SSH_KEY_IDS to the comma-separated DigitalOcean SSH key id(s) or fingerprint(s)."
  echo "Example: export DO_SSH_KEY_IDS=aa:bb:cc:..."
  exit 1
fi

doctl compute droplet create "${NAME}" \
  --region "${REGION}" \
  --size "${SIZE}" \
  --image "${IMAGE}" \
  --ssh-keys "${SSH_KEYS}" \
  --tag-names "${TAGS}" \
  --wait \
  --format ID,Name,PublicIPv4,Status,Region,Image,SizeSlug

