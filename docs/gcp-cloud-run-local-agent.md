# GCP Cloud Run + Mac Local Agent

This deployment keeps the Flask team portal on Google Cloud Run while Mac-only capabilities stay on the host Mac behind the public Mac portal tunnel.

For every release, start from [docs/release-checklist.md](/Users/NPTSG0388/Documents/New%20project/docs/release-checklist.md) so Cloud Run, the Mac local-agent, and any Mac-hosted stack updates are not missed.

## Target Shape

```text
Cloud Run team portal
  -> Google OAuth
  -> Mac local-agent through the public Mac portal tunnel by default
       -> Mac-local cache and SQLite DB
       -> Codex CLI
       -> SeaTalk desktop data
       -> BPMIS API through the Mac VPN
```

Use `BPMIS_CALL_MODE=local_agent` when Cloud Run cannot reach BPMIS directly but the Mac can reach it through VPN.

New Cloud Run services must default to local-agent-backed state. For cache, SQLite DBs, Source Code Q&A repos/indexes, sessions, attachments, runtime evidence, BPMIS project/config rows, PRD briefing stores, Team Dashboard job state, and similar durable data, use the Mac-local data root through local-agent APIs. Do not create a separate Cloud Run-owned team portal cache/DB as the system of record unless the user explicitly asks for Cloud Run-owned storage.

## Mac Host

Configure `.env` on the Mac:

```bash
LOCAL_AGENT_HOST=127.0.0.1
LOCAL_AGENT_PORT=7007
LOCAL_AGENT_PUBLIC_URL=https://app.bankpmtool.uk
LOCAL_AGENT_HMAC_SECRET=<shared-random-secret>
LOCAL_AGENT_SOURCE_CODE_QA_ENABLED=true
LOCAL_AGENT_SEATALK_ENABLED=true
LOCAL_AGENT_BPMIS_ENABLED=true
LOCAL_AGENT_TEAM_PORTAL_DATA_DIR=/absolute/path/to/team-portal-data
SOURCE_CODE_QA_QUERY_SYNC_MODE=background
BPMIS_CALL_MODE=local_agent
```

Start the local capability server. Public access to it should go through the Mac-hosted team portal Cloudflare Tunnel proxy, not a standalone local-agent ngrok tunnel:

```bash
./scripts/run_local_agent.sh start
curl http://127.0.0.1:7007/healthz
curl https://app.bankpmtool.uk/api/local-agent/healthz
```

The local-agent reads the same Mac-local state as the current portal:

- Codex CLI login and synced repos under `LOCAL_AGENT_TEAM_PORTAL_DATA_DIR/source_code_qa`
- Set `LOCAL_AGENT_TEAM_PORTAL_DATA_DIR` to the stable Mac data directory that already contains `team_portal.db`, `source_code_qa/repos`, `source_code_qa/indexes`, Source Code Q&A sessions/attachments/runtime evidence, and BPMIS project/config rows. Do not let new code or deploy scripts use Cloud Run `/tmp/team-portal` as a state store.
- New local-agent-backed features should add their durable cache/DB/files under this Mac data directory and expose them through local-agent endpoints when Cloud Run needs access.
- SeaTalk app and data from `SEATALK_LOCAL_APP_PATH` / `SEATALK_LOCAL_DATA_DIR`
- Source Code Q&A GitLab token from `SOURCE_CODE_QA_GITLAB_TOKEN`

## Cloud Run

Recommended required secrets:

```bash
gcloud secrets create team-portal-flask-secret --data-file=-
gcloud secrets create team-portal-config-encryption-key --data-file=-
gcloud secrets create google-oauth-client-secret-json --data-file=/absolute/path/to/google-client-secret.json
gcloud secrets create local-agent-hmac-secret --data-file=-
```

Deploy the portal:

```bash
CLOUD_RUN_SERVICE=team-portal \
CLOUD_RUN_REGION=asia-southeast1 \
TEAM_PORTAL_BASE_URL=https://your-cloud-run-or-custom-domain \
LOCAL_AGENT_BASE_URL=https://app.bankpmtool.uk \
./scripts/deploy_cloud_run.sh
```

For quick validation without starting a Cloud Build, run the same command with:

```bash
CLOUD_RUN_DEPLOY_DRY_RUN=1 ./scripts/deploy_cloud_run.sh
```

For routine redeploys, the script records a `TEAM_PORTAL_DEPLOY_HASH` on the Cloud Run revision. If you want the script to skip Cloud Build when the local source plus deploy env are unchanged, use:

```bash
CLOUD_RUN_SKIP_UNCHANGED=1 ./scripts/deploy_cloud_run.sh
```

If CI or an operator has already built and pushed an image to Artifact Registry, deploy that exact image digest/tag and skip the local source build:

```bash
CLOUD_RUN_IMAGE=asia-southeast1-docker.pkg.dev/PROJECT/REPO/team-portal:TAG \
./scripts/deploy_cloud_run.sh
```

This still applies the same Cloud Run environment variables and secret references; it only replaces `--source .` with `--image`.

To build that image through Cloud Build first, use the opt-in helper:

```bash
GOOGLE_CLOUD_PROJECT=PROJECT \
CLOUD_RUN_REGION=asia-southeast1 \
CLOUD_RUN_ARTIFACT_REPOSITORY=team-portal \
CLOUD_RUN_IMAGE_NAME=team-portal \
CLOUD_RUN_IMAGE_TAG=manual-$(date +%Y%m%d-%H%M%S) \
./scripts/build_cloud_run_image.sh
```

The helper only builds and pushes the image. It does not deploy by itself; use the printed `CLOUD_RUN_IMAGE=... ./scripts/deploy_cloud_run.sh` command when you are ready.

Attach file/env secrets:

```bash
gcloud run services update team-portal \
  --region asia-southeast1 \
  --set-secrets /secrets/google/client_secret.json=google-oauth-client-secret-json:latest \
  --set-secrets FLASK_SECRET_KEY=team-portal-flask-secret:latest,TEAM_PORTAL_CONFIG_ENCRYPTION_KEY=team-portal-config-encryption-key:latest,LOCAL_AGENT_HMAC_SECRET=local-agent-hmac-secret:latest \
  --set-env-vars GOOGLE_OAUTH_CLIENT_SECRET_FILE=/secrets/google/client_secret.json
```

Use `./scripts/deploy_cloud_run_full.sh` for first-time bootstrap or when secrets/IAM may need repair. To keep repeated full deploys predictable, it reuses the existing Flask secret by default and only adds new Secret Manager versions when values change. Set `CLOUD_RUN_ROTATE_FLASK_SECRET=1` to intentionally rotate Flask sessions, or `CLOUD_RUN_FORCE_SECRET_VERSION=1` to force new secret versions.

## Deployment Speed Notes

Current bottlenecks:

- `gcloud run deploy --source .` packages the local source, runs Cloud Build, and stores the resulting image in Artifact Registry.
- Rebuilding dependencies is expensive when `requirements-cloud-run.txt` or the dependency layer changes.
- Repeated first-time bootstrap work can be slow if every run enables services, reapplies IAM, or creates new secret versions.

Fast paths now available:

- `CLOUD_RUN_DEPLOY_DRY_RUN=1` validates local config without starting Cloud Build.
- `CLOUD_RUN_SKIP_UNCHANGED=1` skips a no-op source deploy when the runtime source, image value, and deploy env match the last revision hash.
- `CLOUD_RUN_IMAGE=...` deploys a prebuilt Artifact Registry image and skips the source-build step.
- `./scripts/build_cloud_run_image.sh` is an opt-in Cloud Build image path; it does not change the default source deploy.
- `CLOUD_RUN_SKIP_SERVICE_ENABLE=1` and `CLOUD_RUN_SKIP_IAM_BINDINGS=1` can trim repeated full-bootstrap checks after the project is already configured.
- Runtime tuning is opt-in through the deploy scripts: `CLOUD_RUN_MIN_INSTANCES`, `CLOUD_RUN_CPU_BOOST`, `CLOUD_RUN_CPU`, `CLOUD_RUN_MEMORY`, `CLOUD_RUN_CONCURRENCY`, and `CLOUD_RUN_TIMEOUT` are passed to `gcloud run deploy` only when set.
- Prebuilt image builds can also be tuned without changing defaults: `CLOUD_RUN_BUILD_MACHINE_TYPE`, `CLOUD_RUN_BUILD_TIMEOUT`, and `CLOUD_RUN_BUILD_DISK_SIZE` are passed to Cloud Build only when set.
- Local-agent connection setup has its own timeout knob: `LOCAL_AGENT_CONNECT_TIMEOUT_SECONDS` defaults to `10`, while `LOCAL_AGENT_TIMEOUT_SECONDS` remains the full read timeout for long Source Code Q&A jobs.

Recommended speed/stability profiles:

- Routine code deploy with no runtime change: `CLOUD_RUN_SKIP_UNCHANGED=1 ./scripts/deploy_cloud_run.sh`.
- Fast redeploy after a prebuilt image: `CLOUD_RUN_IMAGE=asia-southeast1-docker.pkg.dev/... ./scripts/deploy_cloud_run.sh`.
- Faster prebuilt image builds when dependencies or Docker cache are cold: `CLOUD_RUN_BUILD_MACHINE_TYPE=e2-highcpu-8 ./scripts/build_cloud_run_image.sh`, then deploy the printed `CLOUD_RUN_IMAGE=...` command.
- The default Cloud Build config uses BuildKit inline cache plus `latest` and `buildcache` image tags, so dependency layers are reused when `Dockerfile` and `requirements-cloud-run.txt` are unchanged.
- The full UAT/live release orchestrator reuses the newest existing SHA image when the current commit only changed docs, tests, or release scripts. That avoids waiting for a first SHA image build when the Cloud Run container bytes did not change.
- Lower cold-start latency for the shared portal: set `CLOUD_RUN_MIN_INSTANCES=1` and `CLOUD_RUN_CPU_BOOST=true`. This improves first-hit responsiveness, but it can increase Cloud Run cost.
- Faster failure on a broken public local-agent tunnel: set `LOCAL_AGENT_CONNECT_TIMEOUT_SECONDS=3` or `5` while keeping `LOCAL_AGENT_TIMEOUT_SECONDS=300` for long-running Source Code Q&A responses.

For Source Code Q&A, Cloud Run deploys set `SOURCE_CODE_QA_QUERY_SYNC_MODE=background` by default. User questions start against the last usable Mac-local index while the Mac local-agent queues the daily freshness check in the background, so repo clone/pull/index work no longer blocks the answer path.

To improve repeat-query latency without reducing answer quality, warm the answer cache with the same deep query path used by real users:

```bash
TEAM_PORTAL_DATA_DIR=/path/to/.team-portal \
./scripts/source_code_qa_warm_answer_cache.py --from-recent-slow --limit 5
```

The ops summary reports `llm_cache_hits` alongside p50/p95 latency so cache effectiveness is visible in `./scripts/run_team_stack.sh doctor`.

The source upload is also trimmed by `.gcloudignore`: docs, tests, eval fixtures, local caches, runtime data, SQLite files, logs, and secrets are excluded from source deploy uploads. Runtime folders such as `bpmis_jira_tool`, `config`, `prd_briefing`, `static`, and `templates` remain included.

Add this OAuth redirect in Google Cloud Console:

```text
https://your-cloud-run-or-custom-domain/auth/google/callback
```

## Verification

```bash
curl https://your-cloud-run-or-custom-domain/healthz
curl https://app.bankpmtool.uk/api/local-agent/healthz
./scripts/run_local_agent.sh status
```

Then verify in the portal:

- Google OAuth login succeeds.
- BPMIS setup and create flow succeeds through the Mac local-agent proxy.
- Source Code Q&A with Codex returns an answer from the Mac local-agent.
- SeaTalk Summary reads the Mac SeaTalk desktop data through the local-agent.
