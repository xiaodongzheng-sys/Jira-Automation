# Release Checklist

Use this checklist for every portal release. The project now has two runtime surfaces that must stay in sync:

- Cloud Run serves the team portal.
- The Mac local-agent owns Mac-only capabilities, including Source Code Q&A repos/indexes, Codex CLI access, SeaTalk desktop data, and VPN-only BPMIS calls.

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
LOCAL_AGENT_BASE_URL
LOCAL_AGENT_PUBLIC_URL
LOCAL_AGENT_HMAC_SECRET
LOCAL_AGENT_SOURCE_CODE_QA_ENABLED
LOCAL_AGENT_SEATALK_ENABLED
LOCAL_AGENT_BPMIS_ENABLED
SOURCE_CODE_QA_QUERY_SYNC_MODE
BPMIS_CALL_MODE
```

## 2. Cloud Run Release

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
TEAM_PORTAL_DATA_DIR=/tmp/team-portal
SOURCE_CODE_QA_QUERY_SYNC_MODE=background
BPMIS_CALL_MODE=local_agent
LOCAL_AGENT_MODE=sync
LOCAL_AGENT_SOURCE_CODE_QA_ENABLED=true
LOCAL_AGENT_SEATALK_ENABLED=true
LOCAL_AGENT_BPMIS_ENABLED=true
```

- Do not point Cloud Run at a localhost local-agent URL. Cloud Run needs the public Mac local-agent URL, normally from `LOCAL_AGENT_PUBLIC_URL` or `CLOUD_RUN_LOCAL_AGENT_BASE_URL`. If those are not set and `LOCAL_AGENT_BASE_URL` is localhost, the deploy scripts fall back to non-localhost `TEAM_PORTAL_BASE_URL` because the Mac portal exposes `/api/local-agent/*` as a proxy.

## 3. Mac Local-Agent Release

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

- Confirm `LOCAL_AGENT_TEAM_PORTAL_DATA_DIR` points at the durable Mac data directory that contains Source Code Q&A repos and indexes. Do not rely on Cloud Run `/tmp/team-portal` for repo/index state.

## 4. Mac-Hosted Portal Release

If the release also updates the Mac-hosted shared portal stack, update the host workspace and restart the supervised stack:

```bash
cd ~/Workspace/jira-creation-stack-host
git pull --ff-only
./scripts/run_team_stack.sh restart
./scripts/run_team_stack.sh doctor
```

The doctor check should pass before treating the Mac-hosted portal/ngrok stack as live. It verifies portal health, public URL health, ngrok inspector health, revision alignment, data directory readiness, and launchd friendliness.

## 5. Post-Release Acceptance

Run these after Cloud Run and the Mac local-agent are both updated:

- Cloud Run `/healthz` returns the expected revision and deploy hash.
- Mac local-agent `/healthz` reports enabled capabilities for Source Code Q&A, SeaTalk, and BPMIS proxy when those are expected.
- Cloud Run `gcloud run services describe` reports the latest ready revision serving `100%` traffic, and `TEAM_PORTAL_DEPLOY_HASH` matches the deploy script's local hash.
- Cloud Run `/api/local-agent/healthz` returns `source_code_qa: true` and `codex_ready: true` through the public Mac local-agent path.
- Google OAuth login returns to the released portal URL.
- BPMIS Setup can save/load config through the local-agent when Cloud Run uses BPMIS proxy mode.
- BPMIS Create Jira succeeds with Jira-resolvable NPT user emails in owner fields.
- Source Code Q&A with Codex answers through the Mac local-agent and does not block on repo clone/pull/index work.
- Source Code Q&A attachment smoke passes for one small text file; for image-capable releases, confirm Codex mode receives the image through the Mac local-agent path.
- Source Code Q&A active repo config contains the expected GitLab repositories, not fixture/demo `git.example.com` URLs, and index health is `ready`.
- SeaTalk Summary reads Mac desktop data through the local-agent.
- For the Mac-hosted stack, `./scripts/run_team_stack.sh doctor` is clean.

## 6. Easy-To-Miss Release Surfaces

- `scripts/deploy_cloud_run.sh` and `scripts/deploy_cloud_run_full.sh` are included in the Cloud Run deploy hash. Changes there can require a redeploy even when app code is unchanged.
- `local_agent.py`, `bpmis_jira_tool/local_agent_server.py`, `bpmis_jira_tool/local_agent_client.py`, and remote store/service code require a Mac local-agent update, not only Cloud Run.
- Source Code Q&A index/retrieval changes often need both Cloud Run and Mac local-agent updates because Cloud Run owns the web request path while the Mac owns durable repos/indexes and Codex execution.
- BPMIS proxy changes need the Cloud Run env (`BPMIS_CALL_MODE=local_agent`, local-agent URL/secret) and the Mac local-agent capability (`LOCAL_AGENT_BPMIS_ENABLED=true`) to agree.
- SeaTalk changes need the Mac local-agent restarted because Cloud Run cannot read the Mac desktop data directly.
- OAuth/base URL changes need Google Cloud Console callback URLs to match the released hostname.

## 7. Rollback Notes

- Cloud Run rollback: redeploy a known-good image or source revision with `./scripts/deploy_cloud_run.sh`.
- Mac local-agent rollback: check out the known-good commit in `~/Workspace/jira-creation-stack-host`, then restart `run_local_agent` and its tunnel.
- Mac-hosted portal rollback: check out the known-good commit in the host workspace, then run `./scripts/run_team_stack.sh restart` and `./scripts/run_team_stack.sh doctor`.

## 8. Keep This Checklist Current

Whenever a new production, deployment, local-agent, Cloud Run, BPMIS proxy, Source Code Q&A, SeaTalk, OAuth, ngrok, launchd, or host-workspace issue is found, update this checklist in the same fix cycle.

Each update should capture:

- the symptom users/operators saw
- the root cause or strongest confirmed cause
- the command, health check, environment value, file path, or release step that would catch it next time
- whether the issue affects Cloud Run, the Mac local-agent, the Mac-hosted stack, or more than one surface

Do not leave recurring release knowledge only in chat history. If it can prevent a future missed deployment step, add it here.
