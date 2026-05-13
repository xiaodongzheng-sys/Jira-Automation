# Release Evidence: d8fb5fb

Date: 2026-05-13 23:33:04 SGT

Target revision: `d8fb5fb59c743dadfce1f8a106a7846c8ebe2fbc`

## Verified Status

- UAT URL: `https://uat---team-portal-ekaykywtvq-as.a.run.app`
- UAT Cloud Run revision: `team-portal-00301-viv`
- UAT `/healthz/`: `revision=d8fb5fb59c743dadfce1f8a106a7846c8ebe2fbc`, `status=ok`
- Public Live URL: `https://app.bankpmtool.uk`
- Public Live `/healthz`: `revision=d8fb5fb59c743dadfce1f8a106a7846c8ebe2fbc`, `status=ok`
- Cloud Run service live traffic: `team-portal-00200-n7q`, `percent=100`
- Direct local-agent health: `status=ok`, `source_code_qa=true`, `codex_ready=true`
- Public local-agent proxy health: `status=ok`, `source_code_qa=true`, `codex_ready=true`

## Gates

- Read-only UAT/Live smoke: `pass`
- Smoke command: `./.venv/bin/python scripts/run_system_full_test_gate.py --smoke-only --uat-url https://uat---team-portal-ekaykywtvq-as.a.run.app --live-url https://app.bankpmtool.uk --expected-revision d8fb5fb59c743dadfce1f8a106a7846c8ebe2fbc --expect-live-promoted`
- Source Code QA release gate: `pass`
- Source Code QA eval report: `.team-portal/source_code_qa/eval_runs/source_code_qa_eval_20260513T153257Z.json`

## Notes

- Public Live is the Mac/Cloudflare portal. It is intentionally separate from Cloud Run service live traffic.
- Cloud Run UAT and public Live both served `d8fb5fb59c743dadfce1f8a106a7846c8ebe2fbc` during verification.
- The `resume-uat-release-validation` heartbeat was paused after this release was verified to avoid a duplicate UAT/Live run.
