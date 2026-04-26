# GCP Cloud Run + Mac Local Agent

This deployment keeps the Flask team portal on Google Cloud Run while Mac-only capabilities stay on the host Mac behind a fixed ngrok URL.

## Target Shape

```text
Cloud Run team portal
  -> Google OAuth
  -> BPMIS API directly from GCP
  -> Mac local-agent through fixed ngrok URL
       -> Codex CLI
       -> SeaTalk desktop data
```

`BPMIS_CALL_MODE=direct` is the first assumption. Only switch BPMIS to the local-agent path later if Cloud Run cannot reach BPMIS.

## Mac Host

Configure `.env` on the Mac:

```bash
LOCAL_AGENT_HOST=127.0.0.1
LOCAL_AGENT_PORT=7007
LOCAL_AGENT_PUBLIC_URL=https://your-fixed-agent-domain.ngrok.app
LOCAL_AGENT_HMAC_SECRET=<shared-random-secret>
LOCAL_AGENT_SOURCE_CODE_QA_ENABLED=true
LOCAL_AGENT_SEATALK_ENABLED=true
LOCAL_AGENT_BPMIS_ENABLED=false
BPMIS_CALL_MODE=direct
```

Start the local capability server and tunnel:

```bash
./scripts/run_local_agent.sh start
./scripts/run_local_agent_tunnel.sh start
curl http://127.0.0.1:7007/healthz
```

The local-agent reads the same Mac-local state as the current portal:

- Codex CLI login and synced repos under `TEAM_PORTAL_DATA_DIR/source_code_qa`
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
LOCAL_AGENT_BASE_URL=https://your-fixed-agent-domain.ngrok.app \
./scripts/deploy_cloud_run.sh
```

Attach file/env secrets:

```bash
gcloud run services update team-portal \
  --region asia-southeast1 \
  --set-secrets /secrets/google/client_secret.json=google-oauth-client-secret-json:latest \
  --set-secrets FLASK_SECRET_KEY=team-portal-flask-secret:latest,TEAM_PORTAL_CONFIG_ENCRYPTION_KEY=team-portal-config-encryption-key:latest,LOCAL_AGENT_HMAC_SECRET=local-agent-hmac-secret:latest \
  --set-env-vars GOOGLE_OAUTH_CLIENT_SECRET_FILE=/secrets/google/client_secret.json
```

Add this OAuth redirect in Google Cloud Console:

```text
https://your-cloud-run-or-custom-domain/auth/google/callback
```

## Verification

```bash
curl https://your-cloud-run-or-custom-domain/healthz
curl https://your-fixed-agent-domain.ngrok.app/healthz
./scripts/run_local_agent.sh status
./scripts/run_local_agent_tunnel.sh status
```

Then verify in the portal:

- Google OAuth login succeeds.
- BPMIS setup and create flow still uses direct `BPMIS_BASE_URL`.
- Source Code Q&A with Codex returns an answer from the Mac local-agent.
- SeaTalk Summary reads the Mac SeaTalk desktop data through the local-agent.
