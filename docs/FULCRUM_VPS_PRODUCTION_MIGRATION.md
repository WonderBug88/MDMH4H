# Fulcrum VPS Production Migration

## Target

- Host: DigitalOcean Ubuntu 24.04 LTS, NYC3, `s-1vcpu-2gb`
- App path: `/opt/fulcrum/MDMH4H`
- Python venv: `/opt/fulcrum/venv`
- Env file: `/etc/fulcrum/fulcrum.env`
- Web service: `fulcrum-web.service`
- Worker service: `fulcrum-sync-worker.service`
- Reverse proxy: Caddy on `80/443`, upstream `127.0.0.1:5093`
- Database: existing Neon `DATABASE_URL`

## Provision Droplet

From a machine with `doctl` authenticated:

```bash
cd MDMH4H
export DO_SSH_KEY_IDS=<digitalocean-ssh-key-id-or-fingerprint>
bash deploy/digitalocean/create_fulcrum_droplet.sh
```

Record the returned `PublicIPv4`; this is the future GoDaddy `fulcrum` A record.

## Deploy Code

Either clone the repo on the VPS or rsync the current checkout:

```bash
ssh root@<vps-ip> 'mkdir -p /opt/fulcrum'
rsync -az --delete --exclude .git --exclude __pycache__ --exclude deploy/logs ./ root@<vps-ip>:/opt/fulcrum/MDMH4H/
```

Create the runtime services:

```bash
ssh root@<vps-ip>
cd /opt/fulcrum/MDMH4H
bash deploy/linux/bootstrap_fulcrum_vps.sh
```

## Secrets

Copy values from the current production `fulcrum.alpha.env` into:

```bash
nano /etc/fulcrum/fulcrum.env
chmod 600 /etc/fulcrum/fulcrum.env
```

Required values include:

- `DATABASE_URL`
- `SECRET_KEY`
- `FULCRUM_INTEGRATION_SECRET`
- `FULCRUM_SHARED_SECRET`
- BigCommerce app credentials and store token
- Google OAuth client credentials
- OpenAI key if AI-assisted review/generation is enabled
- all `https://fulcrum.fulcrumagentics.com/...` callback URLs

## Neon Pre-Cutover Branch

Before changing DNS, create a Neon branch from the current production branch:

```text
pre-fulcrum-vps-cutover-YYYYMMDD
```

Use the Neon Console or Neon CLI. Do not point the app at this branch; it is a restore point before VPS cutover. The app continues using the current production `DATABASE_URL`.

## Apply Schema And Start

```bash
cd /opt/fulcrum/MDMH4H
/opt/fulcrum/venv/bin/python deploy/apply_fulcrum_runtime_schema.py
systemctl restart caddy fulcrum-web fulcrum-sync-worker
systemctl --no-pager --full status caddy fulcrum-web fulcrum-sync-worker
```

## Pre-DNS Smoke Test

Run from any machine before changing GoDaddy:

```bash
cd MDMH4H
FULCRUM_VPS_IP=<vps-ip> bash deploy/linux/verify_fulcrum_vps.sh
```

Then verify readiness detail:

```bash
curl --resolve fulcrum.fulcrumagentics.com:443:<vps-ip> \
  https://fulcrum.fulcrumagentics.com/fulcrum/readiness?store_hash=99oa2tso
```

Expected:

- `status=ok`
- `gsc.ready=true`
- `ga4.ready=true`
- catalog ready
- no stale running sync

## DNS Cutover

In GoDaddy, update:

```text
A  fulcrum  <vps-ip>  600 seconds
```

After propagation:

```bash
curl https://fulcrum.fulcrumagentics.com/fulcrum/health?store_hash=99oa2tso
curl https://fulcrum.fulcrumagentics.com/fulcrum/readiness?store_hash=99oa2tso
```

Keep the Windows Caddy host running but treat it as rollback-only.

## Worker Verification

Queue one Search Console retry from the public host:

```bash
curl -X POST "https://fulcrum.fulcrumagentics.com/fulcrum/integrations/gsc/sync?store_hash=99oa2tso"
journalctl -u fulcrum-sync-worker -f
```

Confirm the latest `app_runtime.integration_sync_runs` row reaches `succeeded` and that Neon size stays below the project limit.

## Reboot Verification

```bash
reboot
```

After the VPS returns:

```bash
systemctl is-active caddy fulcrum-web fulcrum-sync-worker
curl https://fulcrum.fulcrumagentics.com/fulcrum/health?store_hash=99oa2tso
```

## Rollback

If the VPS fails cutover:

1. Point GoDaddy `fulcrum` A record back to `38.13.122.136`.
2. Keep NordVPN disconnected on the Windows host.
3. Run `.\deploy\check_fulcrum_public_route.ps1` on Windows.
4. Re-test public health and readiness.
