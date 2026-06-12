# Public portal on Google Cloud — final setup steps (owner actions)

The code, GCS bucket, and Cloud Run deploy are done. Two short actions need
project-owner / Cloudflare-dashboard powers that the automation account does
not have. After these, the three public pages (Anti-fraud Reports, Version
Plan, Repo Download) work even when the Mac is off.

---

## Step 1 — Grant Cloud Run read access to the artifacts bucket (1 minute)

Cloud Run runs as `576896528088-compute@developer.gserviceaccount.com` and
must read `gs://team-portal-public-civil-partition-492805-v7`.

**Option A (Cloud Console):** Storage → Buckets →
`team-portal-public-civil-partition-492805-v7` → Permissions → Grant access →
principal `576896528088-compute@developer.gserviceaccount.com`, role
**Storage Object Viewer** → Save.

**Option B (terminal, with your own Google login):**

```bash
gcloud auth login   # your xiaodong.zheng@npt.sg owner account
gcloud storage buckets add-iam-policy-binding \
  gs://team-portal-public-civil-partition-492805-v7 \
  --member=serviceAccount:576896528088-compute@developer.gserviceaccount.com \
  --role=roles/storage.objectViewer
```

Verify afterwards (no Mac involved):
`https://team-portal-ekaykywtvq-as.a.run.app/api/business-insights/reports?domain=anti-fraud`
should show each report with an `artifact` block (not null).

---

## Step 2 — Move path routing from the Mac tunnel to a Cloudflare Worker

Today the path routing lives in the `cloudflared` config **on the Mac**, so
when the Mac is off the whole domain is down. A tiny Worker at Cloudflare's
edge fixes this: public paths go straight to Cloud Run; everything else still
goes to the Mac tunnel.

### 2.1 Confirm the tunnel hostname (likely already exists)

The tunnel config already declares `mac-app.bankpmtool.uk → 127.0.0.1:5000`.
In Cloudflare DNS for `bankpmtool.uk`, make sure there is a **proxied CNAME**:

- Name: `mac-app`
- Target: `2caf5580-7078-4d90-944b-324d0b634076.cfargotunnel.com`
- Proxy status: Proxied (orange cloud)

(If it already exists, skip.)

### 2.2 Create the Worker

Cloudflare dashboard → Workers & Pages → Create → Worker → name it
`team-portal-router`, paste this code, Deploy:

```js
const CLOUD_RUN = "https://team-portal-ekaykywtvq-as.a.run.app";
const MAC = "https://mac-app.bankpmtool.uk";

// Paths served by Cloud Run (public surfaces + auth + static).
// Everything else (admin full portal, healthz beacon, sync/generate jobs)
// stays on the Mac tunnel.
const CLOUD_RUN_PATTERNS = [
  /^\/$/,
  /^\/business-insights(\/.*)?$/,
  /^\/api\/business-insights\/(?!reports\/[^/]+\/(generate|ingest)).*/,
  /^\/version-plan(\/.*)?$/,
  /^\/api\/team-dashboard\/version-plan(\/.*)?$/,
  /^\/source-code-qa\/?$/,
  /^\/api\/source-code-qa\/repo-downloads\/.*/,
  /^\/cloud-auth(\/.*)?$/,
  /^\/cloud-static\/.*/,
];

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const useCloudRun = CLOUD_RUN_PATTERNS.some((re) => re.test(url.pathname));
    const origin = useCloudRun ? CLOUD_RUN : MAC;
    const target = new URL(url.pathname + url.search, origin);
    const upstream = new Request(target, request);
    if (useCloudRun) {
      upstream.headers.set("Host", new URL(CLOUD_RUN).hostname);
    }
    return fetch(upstream);
  },
};
```

### 2.3 Attach the route

Worker → Settings → Triggers/Routes → Add route:

- Route: `app.bankpmtool.uk/*`
- Zone: `bankpmtool.uk`

That's it. The Worker now decides routing at the edge:
- Mac ON: everything works as today (admin portal incluido).
- Mac OFF: the 3 public pages + downloads still work from Cloud Run/GCS;
  admin-only paths return an error until the Mac is back.

### 2.4 (Optional cleanup, later)

Once the Worker is confirmed working, the path rules inside
`~/.cloudflared/config.yml` on the Mac are redundant (the Worker never sends
those paths to the tunnel). Leaving them is harmless.

---

## What publishes data to the bucket (already wired, FYI)

- **Business Insights**: every "Refresh data" run on the Mac re-publishes
  `reports.json` + workbooks/visualizations to GCS at the end.
- **Repo bundles**: every bundle build (admin download or sync) re-publishes
  the zip + metadata.
- **Manual/backfill**: `./scripts/project_python.sh scripts/publish_public_artifacts.py`
  (uses `TEAM_PORTAL_PUBLIC_GCS_PUBLISH_BUCKET` + `..._ACCOUNT` from the host .env).
- Version Plan needs no publishing — Cloud Run reads it from Firestore.
