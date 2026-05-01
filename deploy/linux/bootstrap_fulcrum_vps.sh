#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${REPO_DIR:-/opt/fulcrum/MDMH4H}"
VENV_DIR="${VENV_DIR:-/opt/fulcrum/venv}"
ENV_FILE="${ENV_FILE:-/etc/fulcrum/fulcrum.env}"
START_SERVICES="${START_SERVICES:-0}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root: sudo bash deploy/linux/bootstrap_fulcrum_vps.sh"
  exit 1
fi

if [[ ! -f "${REPO_DIR}/requirements-fulcrum-alpha.txt" ]]; then
  echo "Repo not found at ${REPO_DIR}. Clone or rsync MDMH4H there first."
  exit 1
fi

cd "${REPO_DIR}"

apt-get update
apt-get install -y python3 python3-venv python3-pip git curl caddy

if ! id fulcrum >/dev/null 2>&1; then
  useradd --system --home-dir /opt/fulcrum --create-home --shell /usr/sbin/nologin fulcrum
fi

mkdir -p /etc/fulcrum /var/log/fulcrum /var/log/caddy /opt/fulcrum
chown -R fulcrum:fulcrum /opt/fulcrum /var/log/fulcrum

if [[ ! -f "${ENV_FILE}" ]]; then
  install -o root -g root -m 0600 deploy/linux/fulcrum.env.example "${ENV_FILE}"
  echo "Created ${ENV_FILE}. Fill secrets before starting Fulcrum services."
fi

python3 -m venv "${VENV_DIR}"
"${VENV_DIR}/bin/python" -m pip install --upgrade pip wheel
"${VENV_DIR}/bin/python" -m pip install -r requirements-fulcrum-alpha.txt

install -o root -g root -m 0644 deploy/linux/fulcrum-web.service /etc/systemd/system/fulcrum-web.service
install -o root -g root -m 0644 deploy/linux/fulcrum-sync-worker.service /etc/systemd/system/fulcrum-sync-worker.service
install -o root -g root -m 0644 deploy/linux/Caddyfile.fulcrum /etc/caddy/Caddyfile

systemctl daemon-reload
systemctl enable caddy fulcrum-web fulcrum-sync-worker

if [[ "${START_SERVICES}" == "1" ]]; then
  "${VENV_DIR}/bin/python" deploy/apply_fulcrum_runtime_schema.py
  systemctl restart caddy fulcrum-web fulcrum-sync-worker
  systemctl --no-pager --full status caddy fulcrum-web fulcrum-sync-worker
else
  echo "Bootstrap complete. Fill ${ENV_FILE}, then run:"
  echo "  sudo ${VENV_DIR}/bin/python ${REPO_DIR}/deploy/apply_fulcrum_runtime_schema.py"
  echo "  sudo systemctl restart caddy fulcrum-web fulcrum-sync-worker"
fi

