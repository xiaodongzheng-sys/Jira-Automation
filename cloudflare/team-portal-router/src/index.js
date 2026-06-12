// Edge router for app.bankpmtool.uk.
//
// Public surfaces (and the static/auth endpoints they need) are served by
// Cloud Run so they keep working while the Mac host is offline. Everything
// else — the admin full portal, the /healthz deploy beacon, Jira sync and
// report-generation jobs — still goes to the Mac via its Cloudflare tunnel
// hostname (mac-app.bankpmtool.uk).

const CLOUD_RUN = "https://team-portal-ekaykywtvq-as.a.run.app";
const MAC = "https://mac-app.bankpmtool.uk";

const CLOUD_RUN_PATTERNS = [
  /^\/$/,
  /^\/business-insights(\/.*)?$/,
  // BI APIs run on Cloud Run except generate/ingest, which run the Data
  // Workbench generator on the Mac.
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
    // Pass 3xx responses through to the browser (login flows depend on them).
    return fetch(new Request(target, request), { redirect: "manual" });
  },
};
