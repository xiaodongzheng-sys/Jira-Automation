# Bank PM Tool Cloudflare App Router

This Worker makes `https://app.bankpmtool.uk/` an edge-routed hostname.

Cloud-backed paths are sent directly to Cloud Run:

- `/`
- `/version-plan`
- `/api/team-dashboard/version-plan/*`
- `/cloud-auth/*`
- `/cloud-static/*`

All other paths are sent to the Mac-backed tunnel origin:

- `https://mac-app.bankpmtool.uk`

The Mac origin hostname must remain mapped to the `bankpmtool-live` Cloudflare
Tunnel and the local tunnel config must include an ingress rule for
`mac-app.bankpmtool.uk`.

Deploy:

```bash
npx --yes wrangler@latest deploy --config cloudflare/app-router-worker/wrangler.toml
```

Verify:

```bash
curl -fsSI https://app.bankpmtool.uk/ | grep -i x-bankpmtool-router
curl -fsSI https://app.bankpmtool.uk/cloud-static/team_dashboard.js | grep -i x-bankpmtool-router
curl -fsSI https://app.bankpmtool.uk/healthz | grep -i x-bankpmtool-router
```
