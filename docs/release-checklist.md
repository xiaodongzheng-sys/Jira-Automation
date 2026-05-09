# Release Checklist

Use this checklist for every portal release. The default target is UAT only. Do not publish any Live surface unless the user explicitly asks for Live:

- The Mac-hosted portal exposed through Cloudflare Tunnel is the primary teammate entrypoint.
- The Mac host owns Mac-only capabilities and durable portal state, including Source Code Q&A repos/indexes, Codex CLI access, Source Code Q&A sessions/attachments/runtime evidence, BPMIS setup/project rows, SeaTalk desktop data, and VPN-only BPMIS calls.
- Cloud Run tagged revisions provide the UAT environment. UAT uses `--no-traffic --tag uat`, so it does not change Cloud Run live traffic or the Cloudflare Tunnel Live portal.
- New services running on Cloud Run must default to local-agent-backed cache/DB/state. Use the Mac-local data root through local-agent APIs for durable cache, SQLite DBs, Source Code Q&A repos/indexes, PRD stores, Team Dashboard job state, and similar records; do not use the Cloud Run team portal filesystem or container-local DB as the system of record unless explicitly requested.
- If the user says only "deploy", "publish", "release", or "发/发布", deploy UAT only.
- If the user says "发 live", "publish live", "deploy live", or "发布 live" without saying Cloud Run, publish only the Cloudflare Tunnel Live portal.
- Deploy Cloud Run live traffic only when the user explicitly says "live Cloud Run", "Cloud Run live", "publish the cloud version", or equivalent.

## 1. Pre-Release

- Confirm the working tree intentionally contains only release changes:

```bash
git status --short
```

### System Full Test Gate

Run this gate before every portal release. It is intentionally read-only except for local test temp/cache output: it must not create Jira tickets, send Gmail/SeaTalk messages, write BPMIS data, or mutate production portal state.

- Run the one-command local gate first. This executes the governed-code 100% coverage suite, frontend JavaScript syntax checks for checked-in browser scripts, and the deterministic Source Code Q&A release gate:

```bash
./.venv/bin/python scripts/run_system_full_test_gate.py --skip-smoke
```

The gate sets `ENV_FILE=/dev/null` for subprocesses unless you explicitly provide `ENV_FILE`, so broad local tests do not silently load real credentials from `.env`.

Independent JavaScript syntax checks and the deterministic Source Code Q&A release gate run in parallel after the coverage gate passes. Tune only if the host is overloaded:

```bash
./.venv/bin/python scripts/run_system_full_test_gate.py --skip-smoke --parallel-workers 2
```

Use an explicit threshold when validating release tooling changes:

```bash
./.venv/bin/python scripts/run_system_full_test_gate.py --coverage-fail-under 100 --skip-smoke
```

- The Python coverage gate is intentionally strict for the governed release surface configured in `.coveragerc` (`bpmis_jira_tool/config.py`, `bpmis_jira_tool/errors.py`, `bpmis_jira_tool/user_config.py`, `prd_briefing/models.py`, and `prd_briefing/text_generation.py`). Broader all-module coverage is tracked as an advisory baseline until each legacy integration module is made deterministic enough for a real 100% gate; do not exclude business logic, permission checks, release safety checks, or read-only smoke behavior just to raise the percentage.

- If debugging a failed step, the equivalent local commands are:

```bash
ENV_FILE=/dev/null ./.venv/bin/python -m coverage erase
ENV_FILE=/dev/null ./.venv/bin/python -m coverage run -m unittest discover -s tests
ENV_FILE=/dev/null ./.venv/bin/python -m coverage report --fail-under 100
node --check static/gmail_seatalk_demo.js
node --check static/productization_upgrade_summary.js
node --check static/team_dashboard.js
node --check static/meeting_recorder.js
node --check static/prd_self_assessment.js
node --check static/prd_briefing.js
node --check static/source_code_qa.js
ENV_FILE=/dev/null ./.venv/bin/python scripts/run_source_code_qa_release_gate.py
```

- For Source Code Q&A changes, run the release gate/evals that match the changed retrieval or provider behavior. Source Code Q&A is Codex-only for LLM answers and uses the local token hybrid index for semantic retrieval; do not configure legacy AI providers or remote embedding providers for this workflow:

```bash
./.venv/bin/python scripts/run_source_code_qa_release_gate.py
```

- The default Source Code Q&A release gate uses deterministic mock LLM for its main fixture eval so it does not depend on non-interactive Codex CLI availability, login state, or PATH. Use live Codex only as an explicit provider smoke check:

```bash
./.venv/bin/python scripts/run_source_code_qa_release_gate.py --live-llm
```

- The default gate case set is `evals/source_code_qa/release_gate.jsonl`, a stable cross-team release subset. Use the broader golden/scenario matrix as an advisory or targeted regression suite when touching retrieval/index behavior:

```bash
./.venv/bin/python scripts/run_source_code_qa_evals.py --fixture --mock-llm --cases evals/source_code_qa/golden.jsonl --cases evals/source_code_qa/scenario_matrix.jsonl --data-root /tmp/source-code-qa-full-eval
```

- Source Code Q&A fixture evals must use an isolated data root. Never run fixture evals against the main `TEAM_PORTAL_DATA_DIR`, because that can overwrite live repo mappings with `git.example.com` demo repositories:

```bash
./.venv/bin/python scripts/run_source_code_qa_evals.py --fixture --data-root /tmp/source-code-qa-fixture-data
```

- Review deploy-impacting environment changes before building or deploying. Pay special attention to:

```text
TEAM_PORTAL_BASE_URL
TEAM_PORTAL_TUNNEL_PROVIDER
TEAM_PORTAL_CLOUDFLARE_TUNNEL_NAME
TEAM_PORTAL_DATA_DIR
LOCAL_AGENT_TEAM_PORTAL_DATA_DIR
LOCAL_AGENT_BASE_URL
LOCAL_AGENT_PUBLIC_URL
LOCAL_AGENT_HMAC_SECRET
LOCAL_AGENT_SOURCE_CODE_QA_ENABLED
LOCAL_AGENT_SEATALK_ENABLED
LOCAL_AGENT_BPMIS_ENABLED
SOURCE_CODE_QA_QUERY_SYNC_MODE
BPMIS_CALL_MODE
TEAM_PORTAL_STAGE
TEAM_PORTAL_RELEASE_REVISION
```

- After UAT deploy, run the same one-command gate with read-only UAT/Live smoke enabled. `EXPECTED_REVISION` must be the Git SHA deployed by `deploy_cloud_run_uat.sh`; `UAT_URL` is the Cloud Run tag URL printed by that script; `LIVE_URL` is the Cloudflare Tunnel portal URL:

```bash
./.venv/bin/python scripts/run_system_full_test_gate.py \
  --uat-url "$UAT_URL" \
  --live-url "$LIVE_URL" \
  --expected-revision "$EXPECTED_REVISION"
```

If the local full gate was already run for the same commit, use the read-only smoke-only mode for pre-promotion and post-promotion HTTP checks:

```bash
./.venv/bin/python scripts/run_system_full_test_gate.py --smoke-only \
  --uat-url "$UAT_URL" \
  --live-url "$LIVE_URL" \
  --expected-revision "$EXPECTED_REVISION"
```

- The smoke step only sends GET requests to these read-only endpoints and fails if Live is already serving the UAT revision before promotion:

```bash
curl -fsS "$UAT_URL/healthz/"
curl -fsS "$UAT_URL/api/local-agent/healthz"
curl -fsS "$LIVE_URL/healthz"
curl -fsS "$LIVE_URL/api/local-agent/healthz"
```

- After Live promotion, rerun the same gate with `--expect-live-promoted` so the smoke step requires both UAT and Live to serve the promoted revision:

```bash
./.venv/bin/python scripts/run_system_full_test_gate.py \
  --uat-url "$UAT_URL" \
  --live-url "$LIVE_URL" \
  --expected-revision "$EXPECTED_REVISION" \
  --expect-live-promoted
```

- Treat Cloudflare `502`, `530`, and `1033` pages as a release-blocking tunnel failure even if a `cloudflared` process exists. The only acceptable Live public check is a successful `curl -fsS "$LIVE_URL/healthz"` response from the portal. UAT local-agent health depends on the Live domain's `/uat-local-agent` proxy, so a broken Live Cloudflare Tunnel can also make UAT local-agent-backed pages fail.

## 2. UAT Release

Run this for routine releases after changes are committed and pushed to `origin/main`.

- Deploy timing metrics are appended to the Mac data root by default:

```text
.team-portal/run/deploy_timings.jsonl
```

Override the location with `TEAM_DEPLOY_TIMING_FILE=/path/to/deploy_timings.jsonl` when comparing release speed across UAT and Live runs.
The UAT script records both the total script time and stage timings for prebuilt image lookup, service describe, Cloud Run deploy, and UAT host sync.
Print the latest records and averages with:

```bash
./scripts/report_deploy_timings.py --limit 20
```

- Deploy the pushed commit to a Cloud Run tagged UAT revision. The script requires a clean checkout with `HEAD == origin/main`, sets `TEAM_PORTAL_STAGE=uat`, pins `TEAM_PORTAL_RELEASE_REVISION` to the Git SHA, and deploys with `--no-traffic --tag uat`.

```bash
./scripts/deploy_cloud_run_uat.sh
```

For repeated validation of the same commit/env, enable the hash-based no-change skip. The script still syncs/verifies the UAT Mac local-agent so local-agent-backed routes are not left stale:

```bash
CLOUD_RUN_UAT_SKIP_UNCHANGED=1 ./scripts/deploy_cloud_run_uat.sh
```

For faster Cloud Run revision creation, build the image before the release window and deploy the UAT tag from that image:

```bash
./scripts/build_cloud_run_image.sh
CLOUD_RUN_IMAGE=asia-southeast1-docker.pkg.dev/PROJECT/REPO/team-portal:TAG \
./scripts/deploy_cloud_run_uat.sh
```

The GitHub workflow `.github/workflows/cloud-run-image.yml` remains as a manual fallback (`workflow_dispatch`) when these repository variables are configured:

```text
GCP_PROJECT_ID
GCP_WORKLOAD_IDENTITY_PROVIDER
GCP_SERVICE_ACCOUNT
CLOUD_RUN_REGION
CLOUD_RUN_ARTIFACT_REPOSITORY
CLOUD_RUN_IMAGE_NAME
```

The workflow opts into Node 24 actions execution with `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24=true` so the fallback image prebuild path is not surprised by the GitHub-hosted runner Node 20 retirement.

For automatic push builds, use the direct Cloud Build trigger. It uses the authorized GitHub connection and only runs when Cloud Run runtime inputs or image-build inputs change (`Dockerfile`, `requirements-cloud-run.txt`, `app.py`, `local_agent.py`, `bpmis_jira_tool/`, `config/`, `prd_briefing/`, `static/`, `templates/`, `cloudbuild.yaml`, or image-build scripts). Docs, tests, and release-only scripts no longer trigger a first SHA image build.

```bash
GOOGLE_CLOUD_PROJECT=civil-partition-492805-v7 \
CLOUD_BUILD_GITHUB_CONNECTION_NAME=jira-automation-github \
CLOUD_BUILD_GITHUB_REPOSITORY_NAME=jira-automation \
CLOUD_BUILD_GITHUB_REPO_OWNER=xiaodongzheng-sys \
CLOUD_BUILD_GITHUB_REPO_NAME=Jira-Automation \
./scripts/setup_cloud_build_image_trigger.sh
```

The build script defaults to a faster Cloud Build machine and larger disk for manual image builds. Set `CLOUD_RUN_BUILD_MACHINE_TYPE=default` or `CLOUD_RUN_BUILD_DISK_SIZE=default` to use Cloud Build defaults.

`cloudbuild.yaml` uses BuildKit inline cache plus the `latest` and `buildcache` tags. Dependency and apt layers are reused when `requirements-cloud-run.txt` and `Dockerfile` are unchanged.

By default, `deploy_cloud_run_uat.sh` checks Artifact Registry for an image tagged with the current full Git SHA and uses it automatically when present. If the SHA image is missing, it falls back to the normal Cloud Run source deploy. Disable this with `CLOUD_RUN_UAT_AUTO_PREBUILT_IMAGE=0`.

The manual image build script now uses the full Git SHA tag by default, creates the `team-portal` Artifact Registry Docker repository if it is missing, and reads `CLOUD_RUN_DEPLOY_ACCOUNT` from `.env`:

```bash
GOOGLE_CLOUD_PROJECT=civil-partition-492805-v7 ./scripts/build_cloud_run_image.sh
```

When using the standard artifact naming convention, the UAT script can resolve a prebuilt tag directly:

```bash
GOOGLE_CLOUD_PROJECT=PROJECT \
CLOUD_RUN_UAT_PREBUILT_IMAGE_TAG=TAG \
./scripts/deploy_cloud_run_uat.sh
```

The deploy scripts read `GOOGLE_CLOUD_PROJECT` and `CLOUD_RUN_DEPLOY_ACCOUNT` from `.env`, so routine UAT deploys do not depend on a personal interactive `gcloud auth login` session.

- After Cloud Run UAT deploy succeeds, the script syncs the isolated UAT Mac host workspace to the same Git commit, installs host dependencies only when `requirements.txt` changed, initializes the PRD Briefing SQLite schema under the UAT data root, restarts the UAT Mac local-agent on port `7008`, and verifies public UAT local-agent health through the fixed live portal `/uat-local-agent` proxy. This keeps UAT's Cloud Run frontend and UAT local-agent-backed backend/cache code aligned without restarting the live local-agent.

For faster UAT releases, use the one-command orchestrator. It runs the release gate, builds or reuses an image, deploys UAT with the unchanged-deploy skip enabled, overlaps UAT host sync with Cloud Run deploy, and prints recent timing records:

```bash
./scripts/release_uat_fast.sh
```

For routine UAT plus live promotion, use the full one-command orchestrator. It runs the release gate and image preparation in parallel, reuses the most recent SHA image when the current commit did not change Cloud Run runtime inputs, waits for the GitHub SHA image only when a new runtime image is required, falls back to a local image build if needed, deploys UAT from the selected image, runs the read-only smoke, promotes UAT to live, runs the promoted smoke, runs the live doctor, and prints the timing report:

```bash
./scripts/release_uat_live_fast.sh
```

The image reuse check scans recent first-parent commits for the newest existing Artifact Registry SHA image. If `git diff <image-sha>..HEAD` contains no Cloud Run runtime inputs, the release uses that older image while still stamping `TEAM_PORTAL_RELEASE_REVISION` with the current commit. Disable this conservative reuse path with:

```bash
RELEASE_UAT_LIVE_REUSE_IMAGE_WITHOUT_RUNTIME_CHANGES=0 ./scripts/release_uat_live_fast.sh
```

The UAT local-agent sync is change-aware. `CLOUD_RUN_UAT_LOCAL_AGENT_SYNC_MODE=auto` skips local-agent sync/restart for static/template/docs/test/web-shell-only changes, while `full` forces the old behavior and `skip` skips it explicitly. `CLOUD_RUN_UAT_PARALLEL_HOST_SYNC=1` overlaps the full UAT host sync with Cloud Run deployment.

- If `local-agent-uat-hmac-secret` is missing or inaccessible in Secret Manager, UAT automatically switches to the UAT host `.env` fallback by default. The deploy script reads `LOCAL_AGENT_HMAC_SECRET` from the isolated UAT host `.env`, deploys the new `uat` tag with the HMAC as a literal env var, and uses `--set-secrets` for the remaining base secrets so the stale UAT local-agent secret binding is replaced. This is UAT-only and must still use `--no-traffic`. Disable the automatic fallback with `CLOUD_RUN_UAT_AUTO_ENV_FALLBACK_ON_MISSING_SECRET=0`.

- The default UAT Mac host workspace is `~/Workspace/jira-creation-stack-uat-host`, with data under `.team-portal-uat`. Run the setup helper once before the first isolated UAT deploy:

```bash
./scripts/setup_uat_local_agent.sh
```

The setup helper writes a separate `LOCAL_AGENT_HMAC_SECRET` into the UAT host `.env`. Keep Secret Manager `local-agent-uat-hmac-secret` in sync with that value before the first real UAT deploy; `deploy_cloud_run_uat.sh` wires UAT Cloud Run to that secret.

Override it only when the running UAT local-agent checkout is elsewhere:

```bash
CLOUD_RUN_UAT_HOST_WORKSPACE=/path/to/jira-creation-stack-uat-host \
CLOUD_RUN_DEPLOY_ACCOUNT=vertex-ai-user@civil-partition-492805-v7.iam.gserviceaccount.com \
./scripts/deploy_cloud_run_uat.sh
```

- UAT Cloud Run reaches the UAT local-agent through the live portal path proxy: `https://<fixed-live-portal>/uat-local-agent/api/local-agent/*` forwards to `127.0.0.1:7008/api/local-agent/*`. UAT uses Secret Manager secret `local-agent-uat-hmac-secret` by default, not the live `local-agent-hmac-secret`.

- Do not skip the post-deploy UAT local-agent sync for PRD Briefing, BPMIS proxy, Source Code Q&A, SeaTalk, or other local-agent-backed changes. If you must skip it for a Cloud Run-only dry check, set `CLOUD_RUN_UAT_SYNC_LOCAL_AGENT_AFTER_DEPLOY=0` and treat UAT as not fully validated for local-agent-backed workflows.

- If the active personal `gcloud` account works for the current shell, the account override can be omitted. If not, keep the configured deploy service account:

```bash
CLOUD_RUN_DEPLOY_ACCOUNT=vertex-ai-user@civil-partition-492805-v7.iam.gserviceaccount.com \
CLOUD_RUN_UAT_DRY_RUN=1 ./scripts/deploy_cloud_run_uat.sh
```

- If you need to force the UAT host `.env` fallback manually, set `CLOUD_RUN_UAT_LOCAL_AGENT_SECRET_SOURCE=env`. The fallback reads the isolated UAT `LOCAL_AGENT_HMAC_SECRET` from the UAT host workspace, rewrites the Cloud Run secret bindings to the base Flask/config/OAuth secrets, and deploys the UAT local-agent HMAC as an environment variable:

```bash
CLOUD_RUN_DEPLOY_ACCOUNT=vertex-ai-user@civil-partition-492805-v7.iam.gserviceaccount.com \
CLOUD_RUN_UAT_LOCAL_AGENT_SECRET_SOURCE=env \
./scripts/deploy_cloud_run_uat.sh
```

- Verify UAT before asking for Live publication:

```bash
curl https://<uat-tag-url>/healthz/
curl https://<uat-tag-url>/api/local-agent/healthz
```

- Confirm these before treating UAT as passed:
  - UAT URL opens and shows the `UAT` environment badge.
  - UAT `/healthz/` revision equals the intended Git commit.
  - The Mac host workspace `git rev-parse HEAD` equals the intended Git commit.
  - UAT `/api/local-agent/healthz` succeeds through the public `/uat-local-agent` path to the isolated UAT local-agent.
  - The Cloudflare Tunnel Live `/healthz` still serves the old Live revision until promotion.
  - Live Source Code Q&A still answers through the live data root; UAT Source Code Q&A writes only under `.team-portal-uat`.
  - Any changed workflow passes the expected manual smoke checks.

UAT intentionally shares the fixed live portal domain as an ingress path, but its local-agent process and data root are isolated. For local-agent-backed workflows, durable SQLite/cache state lives under the UAT Mac data root, not inside the Cloud Run UAT container and not under the live `.team-portal`. UAT still does not isolate external write effects such as BPMIS, Trello, Jira, Gmail, or SeaTalk actions if a user performs those actions from UAT.

If Google OAuth login must be tested on UAT, add the UAT callback URL in Google Cloud Console:

```text
https://<uat-tag-url>/auth/google/callback
```

## 3. Explicit Cloud Run Live/Backup Release

Skip this section for routine UAT-gated releases. Use it only when the user explicitly asks to deploy Cloud Run live, publish the cloud version, update the cloud backup, or validate Cloud Run live traffic.

- Run a dry-run first to catch missing `gcloud`, base URL, local-agent URL, and deploy-env problems before Cloud Build starts:

```bash
CLOUD_RUN_DEPLOY_DRY_RUN=1 ./scripts/deploy_cloud_run.sh
```

- If the active personal `gcloud` account needs browser reauthentication, use the configured deploy service account for non-interactive release checks:

```bash
CLOUD_RUN_DEPLOY_ACCOUNT=vertex-ai-user@civil-partition-492805-v7.iam.gserviceaccount.com \
CLOUD_RUN_DEPLOY_DRY_RUN=1 ./scripts/deploy_cloud_run.sh
```

- Deploy Cloud Run from source, or deploy a prebuilt image if one was already produced:

```bash
./scripts/deploy_cloud_run.sh
```

```bash
CLOUD_RUN_IMAGE=asia-southeast1-docker.pkg.dev/PROJECT/REPO/team-portal:TAG \
./scripts/deploy_cloud_run.sh
```

- Keep the Cloud Run defaults unless there is a specific reason to override them:

```text
TEAM_PORTAL_DATA_DIR=/workspace/team-portal-runtime
SOURCE_CODE_QA_QUERY_SYNC_MODE=background
BPMIS_CALL_MODE=local_agent
LOCAL_AGENT_MODE=sync
LOCAL_AGENT_SOURCE_CODE_QA_ENABLED=true
LOCAL_AGENT_SEATALK_ENABLED=true
LOCAL_AGENT_BPMIS_ENABLED=true
```

- Do not use `/tmp/team-portal` for new code, deploy scripts, or runtime defaults. Durable portal state must live in the Mac local-agent data directory (`LOCAL_AGENT_TEAM_PORTAL_DATA_DIR`), and Cloud Run should reach it through local-agent APIs instead of treating its container filesystem as the system of record.

- Do not point Cloud Run at a localhost local-agent URL. Cloud Run needs the public Mac local-agent URL, normally from `LOCAL_AGENT_PUBLIC_URL` or `CLOUD_RUN_LOCAL_AGENT_BASE_URL`. If those are not set and `LOCAL_AGENT_BASE_URL` is localhost, the deploy scripts fall back to non-localhost `TEAM_PORTAL_BASE_URL` because the Mac portal exposes `/api/local-agent/*` as a proxy.

## 4. Mac Local-Agent Release

UAT deploys run this automatically by default. Run it manually only when fixing the Mac host outside the UAT script, when the UAT guard was intentionally skipped, or when preparing the Cloudflare Tunnel Live portal.

- Update the host workspace that actually runs the Mac-local services, usually:

```bash
cd ~/Workspace/jira-creation-stack-host
git pull --ff-only
```

- Restart the local-agent when any local-agent code, settings, Source Code Q&A behavior, SeaTalk behavior, or BPMIS proxy behavior changed:

```bash
./scripts/run_local_agent.sh restart
```

- Confirm the local-agent is healthy on loopback and through the portal proxy:

```bash
curl http://127.0.0.1:7007/healthz
curl https://app.bankpmtool.uk/api/local-agent/healthz
./scripts/run_local_agent.sh status
```

- If the teammate-facing portal path uses `BPMIS_CALL_MODE=local_agent`, restart the local-agent even when the visible change is in a portal page that consumes BPMIS proxy data. A stale local-agent process can keep serving old BPMIS serialization, such as Team Dashboard Biz Projects without `status`, which makes zero-Jira BPMIS projects disappear from Under PRD/Pending Live.
- For Team Dashboard or BPMIS proxy releases, smoke-check a PM who has Biz Projects but no Jira tickets, and confirm the local-agent-backed response preserves each project's `status` before calling the Cloudflare Tunnel portal live.

- Confirm `LOCAL_AGENT_TEAM_PORTAL_DATA_DIR` points at the durable Mac data directory that contains `team_portal.db`, Source Code Q&A repos/indexes, sessions, attachments, runtime evidence, and BPMIS project/config rows. Do not rely on Cloud Run container storage for these records.

## 5. Live Promotion

Run this only after the user explicitly confirms UAT passed and asks to publish Live. The promotion script reads the Cloud Run `uat` tag, verifies the tagged revision's `TEAM_PORTAL_RELEASE_REVISION`, refuses to publish if `origin/main` has moved past that UAT commit, fast-forwards the host workspace, validates the new portal revision on an inactive local slot, restarts the live guard, restarts the live local-agent only when local-agent-backed files changed, and verifies `/healthz`.

```bash
CLOUD_RUN_DEPLOY_ACCOUNT=vertex-ai-user@civil-partition-492805-v7.iam.gserviceaccount.com \
./scripts/promote_uat_to_live.sh
```

The script does not change Cloud Run live traffic. The Cloudflare Tunnel URL remains the primary Live portal.

The default `PROMOTE_UAT_RESTART_MODE=auto` still classifies the portal change shape, while `PROMOTE_UAT_LOCAL_AGENT_RESTART_MODE=auto` separately decides whether the live local-agent needs a restart. Static/template/docs/release-script-only changes skip the live local-agent restart. Source Code QA, local-agent, requirements, and PRD briefing backend changes restart it.

Before switching public live, `PROMOTE_UAT_BLUE_GREEN_VALIDATE=1` starts an inactive candidate portal slot on `PROMOTE_UAT_BLUE_GREEN_PORT` (`5001` by default) and checks its `/healthz` revision. This catches bad portal starts before touching the current public slot.

Override only when you have checked the diff:

```bash
PROMOTE_UAT_RESTART_MODE=full ./scripts/promote_uat_to_live.sh
PROMOTE_UAT_RESTART_MODE=portal ./scripts/promote_uat_to_live.sh
PROMOTE_UAT_LOCAL_AGENT_RESTART_MODE=restart ./scripts/promote_uat_to_live.sh
PROMOTE_UAT_BLUE_GREEN_VALIDATE=0 ./scripts/promote_uat_to_live.sh
```

After promotion, run doctor for the full stack view:

```bash
cd ~/Workspace/jira-creation-stack-host
./scripts/run_team_stack.sh doctor
```

When `BPMIS_CALL_MODE=local_agent`, `run_team_stack.sh restart` also restarts the Mac local-agent first so portal BPMIS proxy changes do not run against a stale local-agent process. The doctor check verifies portal health, public URL health, tunnel health, revision alignment, data directory readiness, and launchd friendliness.

For the Cloudflare Tunnel primary-entry setup, confirm these values in the host `.env`:

```text
TEAM_PORTAL_BASE_URL=https://app.bankpmtool.uk
TEAM_PORTAL_TUNNEL_PROVIDER=cloudflare
TEAM_PORTAL_CLOUDFLARE_TUNNEL_NAME=bankpmtool-live
GOOGLE_CLOUD_PROJECT=civil-partition-492805-v7
CLOUD_RUN_DEPLOY_ACCOUNT=vertex-ai-user@civil-partition-492805-v7.iam.gserviceaccount.com
TEAM_PORTAL_HOST=127.0.0.1
TEAM_PORTAL_PORT=5000
TEAM_PORTAL_STAGE=
```

Google OAuth callback URLs must match `TEAM_PORTAL_BASE_URL` exactly:

```text
https://app.bankpmtool.uk/auth/google/callback
```

## 6. Post-Release Acceptance

Run these after the Mac-hosted portal is updated:

- Mac portal loopback `/healthz` returns the expected revision.
- The Cloudflare Tunnel URL opens the same Mac-hosted portal and returns HTTP 200.
- Google OAuth login returns to the Cloudflare Tunnel URL.
- BPMIS Setup can save/load config from the Cloudflare Tunnel portal.
- BPMIS Create Jira succeeds with Jira-resolvable NPT user emails in owner fields.
- Source Code Q&A with Codex answers from the Mac-hosted portal and does not block on repo clone/pull/index work.
- Source Code Q&A attachment smoke passes for one small text file; for image-capable releases, confirm Codex mode receives the image through the Cloudflare Tunnel portal path.
- Source Code Q&A active repo config contains the expected GitLab repositories, not fixture/demo `git.example.com` URLs, and index health is `ready`.
- Source Code Q&A nightly eval has been removed. Use `scripts/run_source_code_qa_release_gate.py` plus `scripts/source_code_qa_ops_summary.py --strict` as the release/health gates.
- SeaTalk Summary reads Mac desktop data from the Mac host.
- `./scripts/run_team_stack.sh doctor` is clean.

Only when the user explicitly requested Cloud Run live deployment or validation, also verify:

- Cloud Run `/healthz` returns the expected revision and deploy hash.
- `gcloud run services describe` reports the latest ready revision serving `100%` traffic, and `TEAM_PORTAL_DEPLOY_HASH` matches the deploy script's local hash.
- Cloud Run `/api/local-agent/healthz` returns `source_code_qa: true` and `codex_ready: true` through the public Mac path.

## 7. Easy-To-Miss Release Surfaces

- UAT deploys restart and verify the isolated UAT Mac local-agent by default, then run `scripts/source_code_qa_ops_summary.py --strict` against the UAT data root to catch fixture/demo Source Code Q&A repo mappings before signoff. If `CLOUD_RUN_UAT_SYNC_LOCAL_AGENT_AFTER_DEPLOY=0` is used, local-agent-backed UAT workflows are not validated until the UAT host workspace is synced, dependencies are installed, PRD cache schema is initialized, UAT local-agent is restarted on port `7008`, the Source Code Q&A ops guard passes, and public `/uat-local-agent/healthz` passes.
- UAT deploys must not be promoted if `origin/main` has advanced beyond the tagged UAT commit. Re-deploy UAT from the latest commit instead.
- Source Code Q&A index/retrieval changes need the Mac-hosted portal restarted because the Mac owns both the primary web request path and durable repos/indexes.
- Local-agent code changes still need the Mac local-agent restarted when Cloud Run backup mode or local-agent-only features are in use.
- BPMIS proxy changes need the Cloudflare Tunnel portal path checked by default; check Cloud Run env only when the user explicitly requested Cloud Run.
- SeaTalk changes need the Mac-hosted portal or relevant Mac watcher restarted because Cloud Run cannot read the Mac desktop data directly.
- `scripts/deploy_cloud_run.sh` and `scripts/deploy_cloud_run_full.sh` matter only for explicit Cloud Run releases.
- OAuth/base URL changes need Google Cloud Console callback URLs to match the released hostname.

## 8. Rollback Notes

- UAT rollback: re-run `./scripts/deploy_cloud_run_uat.sh` from the intended pushed commit. It replaces the `uat` tag without touching Live traffic.
- Cloud Run live rollback, only for explicit Cloud Run live releases: redeploy a known-good image or source revision with `./scripts/deploy_cloud_run.sh`.
- Mac local-agent rollback: check out the known-good commit in `~/Workspace/jira-creation-stack-host`, then restart `run_local_agent` and its tunnel.
- Primary Mac-hosted portal rollback: check out the known-good commit in the host workspace, then run `./scripts/run_team_stack.sh restart` and `./scripts/run_team_stack.sh doctor`.

## 9. Keep This Checklist Current

Whenever a new production, deployment, local-agent, BPMIS proxy, Source Code Q&A, SeaTalk, OAuth, tunnel, launchd, host-workspace, or explicit Cloud Run issue is found, update this checklist in the same fix cycle.

Each update should capture:

- the symptom users/operators saw
- the root cause or strongest confirmed cause
- the command, health check, environment value, file path, or release step that would catch it next time
- whether the issue affects the default Mac-hosted stack, local-agent-only features, explicit Cloud Run releases, or more than one surface

Do not leave recurring release knowledge only in chat history. If it can prevent a future missed deployment step, add it here.
