# Live Release Checklist

This portal has one business environment: Live. Do not create, deploy, validate,
or reference a second environment.

## Before Release

1. Push the intended commit to `origin/main`.
2. Run the read-only system gate:

```bash
./.venv/bin/python scripts/run_system_full_test_gate.py \
  --profile auto --skip-smoke --coverage-fail-under 100
```

3. Check shell syntax and whitespace:

```bash
git diff --check
bash -n scripts/*.sh scripts/lib/*.sh
```

## Deploy Live

Every release updates both Live surfaces: the Mac-hosted Cloudflare Tunnel
portal and Cloud Run Live. The normal entrypoint is:

```bash
./scripts/release_live_only.sh
```

The release script requires `HEAD == origin/main`, runs the system gate, syncs
the Mac host, restarts the guarded stack, and verifies Live `/healthz`.

For a prebuilt Cloud Run image:

```bash
CLOUD_RUN_IMAGE=asia-southeast1-docker.pkg.dev/PROJECT/team-portal/team-portal:TAG \
  ./scripts/deploy_cloud_run.sh
```

Without a prebuilt image, use the normal source deploy:

```bash
./scripts/deploy_cloud_run.sh
```

Cloud Run deployment must use the configured deploy service account and set
`TEAM_PORTAL_STAGE=live`, `VERSION_PLAN_STORE_BACKEND=firestore`, and the Live
release revision. Cloud Run Live traffic must be 100% on the new revision.

## Verification

Verify only Live endpoints:

```bash
curl -fsS https://app.bankpmtool.uk/healthz
curl -fsS https://app.bankpmtool.uk/api/local-agent/healthz
./scripts/run_team_stack.sh doctor
./scripts/release_status.py --strict
```

The health response revision must match the pushed Git SHA. Do not run write
endpoints as part of release validation.

## Rollback

Rollback is a normal Live release of the intended commit. Check out or reset
the source through the standard Git workflow, push the rollback commit to
`origin/main`, then rerun `scripts/release_live_only.sh` and the Cloud Run Live
deploy. Never restore a deleted UAT script or tag.
