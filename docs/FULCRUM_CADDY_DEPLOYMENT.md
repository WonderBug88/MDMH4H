# Fulcrum Stable HTTPS Deployment With Caddy

## Goal

Replace the temporary `loca.lt` tunnel with a stable, free, open-source HTTPS host:

- `https://fulcrum.fulcrumagentics.com`

Fulcrum will continue to run locally on:

- `127.0.0.1:5093`

Caddy will terminate HTTPS and reverse proxy traffic to Fulcrum.

## Files

- `deploy/Caddyfile.fulcrum`
- `deploy/ensure_fulcrum_hosted_stack.ps1`
- `deploy/start_fulcrum_hosted_5093.ps1`
- `deploy/start_caddy_fulcrum.ps1`
- `deploy/check_fulcrum_public_route.ps1`

## DNS

Create an `A` record:

- `fulcrum.fulcrumagentics.com -> 38.13.122.136`

Do not point the root storefront domain at Fulcrum.

## Firewall

Open inbound ports:

- `80`
- `443`

## Install Caddy

Recommended Windows install methods from the official docs:

- Chocolatey: `choco install caddy`
- Scoop: `scoop install caddy`
- Manual binary install from official releases

Official docs:

- `https://caddyserver.com/docs/install`
- `https://caddyserver.com/docs/running`

After install, confirm:

```powershell
caddy version
```

## Start Fulcrum

Run the app behind localhost only:

```powershell
cd C:\Users\juddu\Downloads\PAM\MDMH4H
.\deploy\ensure_fulcrum_hosted_stack.ps1
```

Health check:

```text
http://127.0.0.1:5093/fulcrum/health?store_hash=99oa2tso
```

## Start Caddy

Run Caddy with the Fulcrum config:

```powershell
cd C:\Users\juddu\Downloads\PAM\MDMH4H
.\deploy\start_caddy_fulcrum.ps1
```

Or directly:

```powershell
caddy run --config C:\Users\juddu\Downloads\PAM\MDMH4H\deploy\Caddyfile.fulcrum
```

Once DNS is live and ports are open, Caddy should provision HTTPS automatically.

## Public Health Check

After DNS propagation:

```text
https://fulcrum.fulcrumagentics.com/fulcrum/health?store_hash=99oa2tso
```

Expected:

- HTTP `200`
- Fulcrum JSON health payload

To distinguish local Caddy from public WAN routing:

```powershell
cd C:\Users\juddu\Downloads\PAM\MDMH4H
.\deploy\check_fulcrum_public_route.ps1
```

If `--resolve fulcrum.fulcrumagentics.com:443:127.0.0.1` passes but the public DNS request fails, keep Caddy pointed at `127.0.0.1:5093` and fix router/NAT or Windows Firewall for WAN `80/443 -> 192.168.1.66:80/443`.

## BigCommerce Callback URLs

After the public health check works, update the Developer Portal to:

- `https://fulcrum.fulcrumagentics.com/fulcrum/auth`
- `https://fulcrum.fulcrumagentics.com/fulcrum/load`
- `https://fulcrum.fulcrumagentics.com/fulcrum/uninstall`
- `https://fulcrum.fulcrumagentics.com/fulcrum/remove-user`

Also update:

- `FULCRUM_APP_BASE_URL`
- `FULCRUM_AUTH_CALLBACK_URL`
- `FULCRUM_LOAD_CALLBACK_URL`
- `FULCRUM_UNINSTALL_CALLBACK_URL`
- `FULCRUM_REMOVE_USER_CALLBACK_URL`

in:

- `C:\Users\juddu\Downloads\PAM\fulcrum.alpha.env`

## Recommended Cutover Order

1. Confirm local Fulcrum health on `127.0.0.1:5093`
2. Install Caddy
3. Point `fulcrum.fulcrumagentics.com` to `38.13.122.136`
4. Open ports `80/443`
5. Start Caddy
6. Wait for public HTTPS health to return `200`
7. Update BigCommerce callback URLs
8. Test:
   - open app
   - uninstall callback
   - remove-user callback

## Why This Is Better Than The Tunnel

- stable hostname
- automatic HTTPS
- no tunnel expiration
- open source
- no monthly cost

## Current Temporary Tunnel Note

If you need to keep testing before DNS is ready, the callback host must always match the current working tunnel URL exactly.

