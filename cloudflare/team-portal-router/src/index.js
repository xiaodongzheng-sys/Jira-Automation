// Edge router for app.bankpmtool.uk (worker name: bankpmtool-app-router).
//
// Public surfaces are served by Cloud Run so they keep working while the Mac
// host is offline; everything else — the admin full portal, the /healthz
// deploy beacon, Jira sync and report-generation jobs — goes to the Mac via
// its Cloudflare tunnel hostname.

const CLOUD_ORIGIN = "https://team-portal-ekaykywtvq-as.a.run.app";
const MAC_ORIGIN = "https://mac-app.bankpmtool.uk";

const CLOUD_PATH_PATTERNS = [
  /^\/$/,
  // Business Insights: page + read APIs/artifacts on Cloud Run (GCS-backed).
  // generate/ingest run the Data Workbench generator on the Mac.
  /^\/business-insights(?:\/.*)?$/,
  /^\/api\/business-insights\/(?!reports\/[^/]+\/(?:generate|ingest)).*/,
  // Version Plan: public read view, Firestore-backed on Cloud Run.
  /^\/version-plan(?:\/.*)?$/,
  /^\/api\/team-dashboard\/version-plan(?:\/.*)?$/,
  // Source Code Repo Download: public page + GCS-backed bundle downloads.
  // All other /api/source-code-qa/* (admin chat/config/sync) stay on the Mac.
  /^\/source-code-qa\/?$/,
  /^\/api\/source-code-qa\/repo-downloads\/.*/,
  /^\/cloud-auth(?:\/.*)?$/,
  /^\/cloud-static(?:\/.*)?$/,
];

function shouldUseCloud(pathname) {
  return CLOUD_PATH_PATTERNS.some((pattern) => pattern.test(pathname));
}

function routedUrl(requestUrl, origin) {
  const url = new URL(requestUrl);
  const target = new URL(origin);
  target.pathname = url.pathname;
  target.search = url.search;
  return target;
}

function routedHeaders(request, targetHost) {
  const headers = new Headers(request.headers);
  headers.set("X-Forwarded-Host", new URL(request.url).host);
  headers.set("X-Forwarded-Proto", "https");
  headers.set("X-BankPMTool-Origin", targetHost);
  headers.delete("Host");
  return headers;
}

function rewriteLocation(value) {
  if (!value) {
    return value;
  }
  return value
    .replaceAll(CLOUD_ORIGIN, "https://app.bankpmtool.uk")
    .replaceAll(MAC_ORIGIN, "https://app.bankpmtool.uk");
}

async function proxy(request, origin) {
  const targetUrl = routedUrl(request.url, origin);
  const upstream = await fetch(targetUrl, {
    method: request.method,
    headers: routedHeaders(request, targetUrl.host),
    body: request.body,
    redirect: "manual",
  });
  const headers = new Headers(upstream.headers);
  if (headers.has("Location")) {
    headers.set("Location", rewriteLocation(headers.get("Location")));
  }
  headers.set("X-BankPMTool-Router", origin === CLOUD_ORIGIN ? "cloud" : "mac");
  return new Response(upstream.body, {
    status: upstream.status,
    statusText: upstream.statusText,
    headers,
  });
}

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const origin = shouldUseCloud(url.pathname) ? CLOUD_ORIGIN : MAC_ORIGIN;
    return proxy(request, origin);
  },
};
