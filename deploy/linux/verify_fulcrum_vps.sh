#!/usr/bin/env bash
set -euo pipefail

HOSTNAME="${HOSTNAME:-fulcrum.fulcrumagentics.com}"
STORE_HASH="${STORE_HASH:-99oa2tso}"
VPS_IP="${1:-${FULCRUM_VPS_IP:-}}"

if [[ -z "${VPS_IP}" ]]; then
  echo "Usage: FULCRUM_VPS_IP=<ip> bash deploy/linux/verify_fulcrum_vps.sh"
  echo "   or: bash deploy/linux/verify_fulcrum_vps.sh <ip>"
  exit 1
fi

paths=(
  "/fulcrum/health?store_hash=${STORE_HASH}"
  "/fulcrum/readiness?store_hash=${STORE_HASH}"
  "/static/fulcrum/route-authority-logo.png"
  "/fulcrum/setup?store_hash=${STORE_HASH}"
  "/fulcrum/admin/developer?store_hash=${STORE_HASH}"
)

for path in "${paths[@]}"; do
  url="https://${HOSTNAME}${path}"
  echo "[check] ${url} via ${VPS_IP}"
  curl --silent --show-error --fail --resolve "${HOSTNAME}:443:${VPS_IP}" "${url}" >/dev/null
done

echo "Fulcrum VPS HTTPS smoke checks passed for ${VPS_IP}."

