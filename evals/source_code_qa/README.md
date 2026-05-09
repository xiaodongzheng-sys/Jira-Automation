# Source Code Q&A evals

This folder holds golden retrieval cases for the Source Code Q&A product.

Each JSONL row is one user question plus objective checks:

```json
{"id":"af-bpmis-batch-create","pm_team":"AF","country":"All","question":"where is batchCreateJiraIssue implemented","expected_paths":["bpmis/jira_client.py"],"required_terms":["batchCreateJiraIssue"]}
```

Index and graph upgrades can be checked directly in the same row. Optional fields include `expected_parser_backend`, `min_tree_sitter_files`, `expected_symbol_edges`, and `expected_repo_graph_edges`:

```json
{"id":"cross-repo-http-path","pm_team":"AF","country":"All","question":"which repo handles remote issue client API call","expected_parser_backend":"tree_sitter+regex","min_tree_sitter_files":4,"expected_repo_graph_edges":[{"from_repo":"Portal Repo","to_repo":"Issue Service","edge_kind":"http_path","min_confidence":0.8}]}
```

Run against the synced portal data:

```bash
PYTHONPATH=. ./.venv/bin/python scripts/run_source_code_qa_evals.py --cases evals/source_code_qa/golden.jsonl
PYTHONPATH=. ./.venv/bin/python scripts/run_source_code_qa_evals.py --cases evals/source_code_qa/golden.jsonl --cases evals/source_code_qa/scenario_matrix.jsonl
PYTHONPATH=. ./.venv/bin/python scripts/run_source_code_qa_evals.py --fixture --cases evals/source_code_qa/golden.jsonl --cases evals/source_code_qa/scenario_matrix.jsonl
```

Turn user feedback into draft eval candidates:

```bash
python3 scripts/source_code_qa_feedback_to_eval.py --output evals/source_code_qa/feedback_candidates.jsonl
python3 scripts/source_code_qa_feedback_to_eval.py --include-useful --json --output evals/source_code_qa/feedback_candidates.jsonl
```

Build automatic broad-quality candidates from live telemetry and feedback without asking users for more input:

```bash
PYTHONPATH=. ./.venv/bin/python scripts/source_code_qa_auto_eval_candidates.py --runnable-only --json
```

This selects useful feedback, repeated questions, slow queries, deadline fallbacks, no-match records, and answer-contract risks. Positive/useful and successful telemetry cases become runnable smoke/regression candidates with observed paths and terms. Negative feedback remains review-only so an incorrect observed answer is not frozen as expected behavior.

Review the generated candidates before promoting them into `golden.jsonl`. Negative feedback is included by default; add `--include-useful` if you also want positive smoke-test cases.

Feedback candidates preserve replay context from the original answer: trace id, answer mode, LLM route/model, answer contract status, observed answer preview, evidence count, and observed paths. Negative feedback is intentionally marked `draft_status=needs_human_expected_evidence`; do not promote it as a blocking golden eval until a reviewer adds the corrected `expected_paths`, `required_terms`, `forbidden_terms`, or policy expectations. Positive `useful` feedback can be used as `ready_positive_smoke` coverage because the observed paths are expected to remain present.

After review, promote approved candidates into the real-question eval file:

```bash
python3 scripts/promote_source_code_qa_eval_candidates.py --input evals/source_code_qa/feedback_candidates.jsonl --output evals/source_code_qa/golden_real.jsonl
python3 scripts/promote_source_code_qa_eval_candidates.py --allow-positive-smoke --json
```

Use `draft_status=approved` for negative-feedback candidates after adding objective checks. The promotion command rejects unapproved or assertion-free candidates, deduplicates by id/question, and writes `golden_real.jsonl`. The nightly eval automatically includes `golden_real.jsonl` when the file exists.

`scenario_matrix.jsonl` is the coverage checklist for promoting new cases. Keep at least one positive and one negative case for symbol lookup, API flow, data-source tracing, config lookup, error/root-cause, cross-repo flow, and follow-up context before calling a release broadly improved.

Use this before and after retrieval, prompt, model, or indexing changes. The goal is to improve whole classes of source-code questions without tuning for one-off examples.

For a repeatable local quality check, run:

```bash
PYTHONPATH=. ./.venv/bin/python scripts/run_source_code_qa_broad_eval.py --json
```

The broad eval writes timestamped reports under `TEAM_PORTAL_DATA_DIR/source_code_qa/eval_runs/`, `broad_latest.json`, and `TEAM_PORTAL_DATA_DIR/run/source_code_qa_broad_eval.json`. It runs `release_gate.jsonl` plus `scenario_matrix.jsonl` on deterministic fixtures, runs the mock-LLM answer smoke eval, generates automatic candidates from live telemetry/feedback, and evaluates runnable candidates against the synced repo data. Auto-candidate failures are reported as broad-quality warnings and do not block the release gate.

Before publishing Source Code Q&A retrieval, prompt, or index changes, run the release gate:

```bash
PYTHONPATH=. ./.venv/bin/python scripts/run_source_code_qa_release_gate.py --include-useful-feedback
```

The release gate remains the stable blocking layer. It enforces minimum coverage and zero-failure thresholds on `release_gate.jsonl`, writes `TEAM_PORTAL_DATA_DIR/run/source_code_qa_release_gate.json`, and uses the deterministic local LLM provider by default so release checks do not depend on the local Codex CLI login/path environment. Pass `--live-llm` only when you intentionally want to validate the configured live provider.

Build the review queue directly when triaging misses:

```bash
PYTHONPATH=. ./.venv/bin/python scripts/source_code_qa_review_queue.py --json
```

Run only the LLM answer smoke eval without calling an external model:

```bash
PYTHONPATH=. ./.venv/bin/python scripts/run_source_code_qa_evals.py --fixture --mock-llm --cases evals/source_code_qa/llm_smoke.jsonl
```

Run the Codex-only fixture profile when validating Source Code Q&A changes:

```bash
PYTHONPATH=. ./.venv/bin/python scripts/run_source_code_qa_evals.py --fixture --mock-llm --cases evals/source_code_qa/golden.jsonl --json
```

Use `--fixture` when you want a deterministic miniature repo set for regression checks. It creates AF and CRMS fixture repositories under the selected data root, then runs the same eval cases against generated code instead of depending on whatever repos happen to be synced locally.

The JSON output includes LLM routing and quality metadata (`llm_provider`, `llm_model`, `llm_route`, `llm_budget_mode`, `answer_claim_check`, and `answer_contract`) so regressions can be grouped by provider, budget, and evidence policy rather than inspected case by case.

Eval summaries also include `failure_buckets` (`retrieval`, `answer_policy`, `answer_content`, `query_status`, or `other`), `coverage_buckets`, `answer_mode_buckets`, `fallback_buckets`, `cache_buckets`, `slow_query_buckets`, and per-case `answer_policies`. Use these buckets to decide whether a failure needs retrieval/index work, policy tuning, answer rendering changes, latency/cache tuning, or broader scenario coverage.

Source Code Q&A now runs on the Codex bridge only. Historical Gemini, Vertex, OpenAI-compatible, and remote embedding profiles have been retired; semantic retrieval uses the local token-hybrid index and does not require an external embedding provider.

The fixture now contains Java, Python, TypeScript, and two AF repos so the eval runner can validate Tree-sitter parser coverage, symbol edges, Feign/service-name matching, and HTTP-path cross-repo graph edges deterministically.

The scenario matrix also includes Chinese business-language prompts for data-source, impact, test-coverage, and operational-boundary questions. Keep these multilingual cases green so business users can ask naturally without knowing the English retrieval vocabulary.
