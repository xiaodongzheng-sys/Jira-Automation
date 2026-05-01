# Release Checklist

Use this checklist for every routine portal release. The default release flow is UAT first, then fixed-ngrok Live only after explicit approval:

- The Mac-hosted portal exposed through the fixed ngrok URL is the primary teammate entrypoint.
- The Mac host owns Mac-only capabilities and durable portal state, including Source Code Q&A repos/indexes, Codex CLI access, Source Code Q&A sessions/attachments/runtime evidence, BPMIS setup/project rows, SeaTalk desktop data, and VPN-only BPMIS calls.
- Cloud Run tagged revisions provide the UAT environment. UAT uses `--no-traffic --tag uat`, so it does not change Cloud Run live traffic or the fixed-ngrok Live portal.
- Fixed-ngrok Live must not be updated until the user explicitly confirms UAT has passed and asks to publish Live.

## 1. Pre-Release

- Confirm the working tree intentionally contains only release changes:

```bash
git status --short
```

- Run the focused tests for the changed area, then the broad suite when the release touches shared portal behavior:

```bash
./.venv/bin/python -m unittest discover -s tests
```

- For Source Code Q&A changes, run the release gate/evals that match the changed retrieval or provider behavior:

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

## 2. UAT Release

Run this for routine releases after changes are committed and pushed to `origin/main`.

- Deploy the pushed commit to a Cloud Run tagged UAT revision. The script requires a clean checkout with `HEAD == origin/main`, sets `TEAM_PORTAL_STAGE=uat`, pins `TEAM_PORTAL_RELEASE_REVISION` to the Git SHA, and deploys with `--no-traffic --tag uat`.

```bash
CLOUD_RUN_DEPLOY_ACCOUNT=vertex-ai-user@civil-partition-492805-v7.iam.gserviceaccount.com \
./scripts/deploy_cloud_run_uat.sh
```

- After Cloud Run UAT deploy succeeds, the script syncs the Mac host workspace to the same Git commit, installs host dependencies, initializes the PRD Briefing SQLite schema under the Mac data root, restarts the Mac local-agent, and verifies public local-agent health. This keeps UAT's Cloud Run frontend and local-agent-backed backend/cache code aligned.

- The default Mac host workspace is `~/Workspace/jira-creation-stack-host`. Override it only when the running local-agent checkout is elsewhere:

```bash
CLOUD_RUN_UAT_HOST_WORKSPACE=/path/to/jira-creation-stack-host \
CLOUD_RUN_DEPLOY_ACCOUNT=vertex-ai-user@civil-partition-492805-v7.iam.gserviceaccount.com \
./scripts/deploy_cloud_run_uat.sh
```

- Do not skip the post-deploy local-agent sync for PRD Briefing, BPMIS proxy, Source Code Q&A, SeaTalk, or other local-agent-backed changes. If you must skip it for a Cloud Run-only dry check, set `CLOUD_RUN_UAT_SYNC_LOCAL_AGENT_AFTER_DEPLOY=0` and treat UAT as not fully validated for local-agent-backed workflows.

- If the active personal `gcloud` account works for the current shell, the account override can be omitted. If not, keep the configured deploy service account:

```bash
CLOUD_RUN_DEPLOY_ACCOUNT=vertex-ai-user@civil-partition-492805-v7.iam.gserviceaccount.com \
CLOUD_RUN_UAT_DRY_RUN=1 ./scripts/deploy_cloud_run_uat.sh
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
  - UAT `/api/local-agent/healthz` succeeds through the public Mac local-agent path.
  - The fixed-ngrok Live `/healthz` still serves the old Live revision until promotion.
  - Any changed workflow passes the expected manual smoke checks.

UAT intentionally shares the existing Mac local-agent public path for Mac-only capabilities. For local-agent-backed workflows, durable SQLite/cache state is on the Mac host, not inside the Cloud Run UAT container. UAT isolates Cloud Run code and traffic, but it does not isolate downstream data or external write effects such as BPMIS, Trello, Jira, Gmail, or SeaTalk actions.

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

UAT deploys run this automatically by default. Run it manually only when fixing the Mac host outside the UAT script, when the UAT guard was intentionally skipped, or when preparing the fixed-ngrok Live portal.

- Update the host workspace that actually runs the Mac-local services, usually:

```bash
cd ~/Workspace/jira-creation-stack-host
git pull --ff-only
```

- Restart the local-agent and its tunnel when any local-agent code, settings, Source Code Q&A behavior, SeaTalk behavior, or BPMIS proxy behavior changed:

```bash
./scripts/run_local_agent.sh restart
./scripts/run_local_agent_tunnel.sh restart
```

- Confirm the local-agent is healthy on loopback and through the public tunnel:

```bash
curl http://127.0.0.1:7007/healthz
curl https://your-fixed-agent-domain.ngrok.app/healthz
./scripts/run_local_agent.sh status
./scripts/run_local_agent_tunnel.sh status
```

- If the teammate-facing portal path uses `BPMIS_CALL_MODE=local_agent`, restart the local-agent even when the visible change is in a portal page that consumes BPMIS proxy data. A stale local-agent process can keep serving old BPMIS serialization, such as Team Dashboard Biz Projects without `status`, which makes zero-Jira BPMIS projects disappear from Under PRD/Pending Live.
- For Team Dashboard or BPMIS proxy releases, smoke-check a PM who has Biz Projects but no Jira tickets, and confirm the local-agent-backed response preserves each project's `status` before calling the fixed ngrok portal live.

- Confirm `LOCAL_AGENT_TEAM_PORTAL_DATA_DIR` points at the durable Mac data directory that contains `team_portal.db`, Source Code Q&A repos/indexes, sessions, attachments, runtime evidence, and BPMIS project/config rows. Do not rely on Cloud Run container storage for these records.

## 5. Live Promotion

Run this only after the user explicitly confirms UAT passed and asks to publish Live. The promotion script reads the Cloud Run `uat` tag, verifies the tagged revision's `TEAM_PORTAL_RELEASE_REVISION`, refuses to publish if `origin/main` has moved past that UAT commit, fast-forwards the host workspace, restarts fixed-ngrok Live, and verifies `/healthz`.

```bash
CLOUD_RUN_DEPLOY_ACCOUNT=vertex-ai-user@civil-partition-492805-v7.iam.gserviceaccount.com \
./scripts/promote_uat_to_live.sh
```

The script does not change Cloud Run live traffic. The fixed-ngrok URL remains the primary Live portal.

After promotion, run doctor for the full stack view:

```bash
cd ~/Workspace/jira-creation-stack-host
./scripts/run_team_stack.sh doctor
```

When `BPMIS_CALL_MODE=local_agent`, `run_team_stack.sh restart` also restarts the Mac local-agent first so portal BPMIS proxy changes do not run against a stale local-agent process. The doctor check verifies portal health, public URL health, ngrok inspector health, revision alignment, data directory readiness, and launchd friendliness.

For the fixed-ngrok primary-entry setup, confirm these values in the host `.env`:

```text
TEAM_PORTAL_BASE_URL=https://<fixed-portal-ngrok-host>
TEAM_PORTAL_HOST=127.0.0.1
TEAM_PORTAL_PORT=5000
TEAM_PORTAL_STAGE=
```

Google OAuth callback URLs must match `TEAM_PORTAL_BASE_URL` exactly:

```text
https://<fixed-portal-ngrok-host>/auth/google/callback
```

## 6. Post-Release Acceptance

Run these after the Mac-hosted portal is updated:

- Mac portal loopback `/healthz` returns the expected revision.
- The fixed ngrok URL opens the same Mac-hosted portal and returns HTTP 200.
- Google OAuth login returns to the fixed ngrok URL.
- BPMIS Setup can save/load config from the fixed ngrok portal.
- BPMIS Create Jira succeeds with Jira-resolvable NPT user emails in owner fields.
- Source Code Q&A with Codex answers from the Mac-hosted portal and does not block on repo clone/pull/index work.
- Source Code Q&A attachment smoke passes for one small text file; for image-capable releases, confirm Codex mode receives the image through the fixed ngrok portal path.
- Source Code Q&A active repo config contains the expected GitLab repositories, not fixture/demo `git.example.com` URLs, and index health is `ready`.
- SeaTalk Summary reads Mac desktop data from the Mac host.
- `./scripts/run_team_stack.sh doctor` is clean.

Only when the user explicitly requested Cloud Run live deployment or validation, also verify:

- Cloud Run `/healthz` returns the expected revision and deploy hash.
- `gcloud run services describe` reports the latest ready revision serving `100%` traffic, and `TEAM_PORTAL_DEPLOY_HASH` matches the deploy script's local hash.
- Cloud Run `/api/local-agent/healthz` returns `source_code_qa: true` and `codex_ready: true` through the public Mac path.

## 7. Easy-To-Miss Release Surfaces

- UAT deploys restart and verify the Mac local-agent by default. If `CLOUD_RUN_UAT_SYNC_LOCAL_AGENT_AFTER_DEPLOY=0` is used, local-agent-backed UAT workflows are not validated until the host workspace is synced, dependencies are installed, PRD cache schema is initialized, local-agent is restarted, and public health passes.
- UAT deploys must not be promoted if `origin/main` has advanced beyond the tagged UAT commit. Re-deploy UAT from the latest commit instead.
- Source Code Q&A index/retrieval changes need the Mac-hosted portal restarted because the Mac owns both the primary web request path and durable repos/indexes.
- Local-agent code changes still need the Mac local-agent restarted when Cloud Run backup mode or local-agent-only features are in use.
- BPMIS proxy changes need the fixed ngrok portal path checked by default; check Cloud Run env only when the user explicitly requested Cloud Run.
- SeaTalk changes need the Mac-hosted portal or relevant Mac watcher restarted because Cloud Run cannot read the Mac desktop data directly.
- `scripts/deploy_cloud_run.sh` and `scripts/deploy_cloud_run_full.sh` matter only for explicit Cloud Run releases.
- OAuth/base URL changes need Google Cloud Console callback URLs to match the released hostname.

## 8. Rollback Notes

- UAT rollback: re-run `./scripts/deploy_cloud_run_uat.sh` from the intended pushed commit. It replaces the `uat` tag without touching Live traffic.
- Cloud Run live rollback, only for explicit Cloud Run live releases: redeploy a known-good image or source revision with `./scripts/deploy_cloud_run.sh`.
- Mac local-agent rollback: check out the known-good commit in `~/Workspace/jira-creation-stack-host`, then restart `run_local_agent` and its tunnel.
- Primary Mac-hosted portal rollback: check out the known-good commit in the host workspace, then run `./scripts/run_team_stack.sh restart` and `./scripts/run_team_stack.sh doctor`.

## 9. Keep This Checklist Current

Whenever a new production, deployment, local-agent, BPMIS proxy, Source Code Q&A, SeaTalk, OAuth, ngrok, launchd, host-workspace, or explicit Cloud Run issue is found, update this checklist in the same fix cycle.

Each update should capture:

- the symptom users/operators saw
- the root cause or strongest confirmed cause
- the command, health check, environment value, file path, or release step that would catch it next time
- whether the issue affects the default Mac-hosted stack, local-agent-only features, explicit Cloud Run releases, or more than one surface

Do not leave recurring release knowledge only in chat history. If it can prevent a future missed deployment step, add it here.
