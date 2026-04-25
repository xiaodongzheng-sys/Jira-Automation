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

Review the generated candidates before promoting them into `golden.jsonl`. Negative feedback is included by default; add `--include-useful` if you also want positive smoke-test cases.

Feedback candidates preserve replay context from the original answer: trace id, answer mode, LLM route/model, answer contract status, observed answer preview, evidence count, and observed paths. Negative feedback is intentionally marked `draft_status=needs_human_expected_evidence`; do not promote it as a blocking golden eval until a reviewer adds the corrected `expected_paths`, `required_terms`, `forbidden_terms`, or policy expectations. Positive `useful` feedback can be used as `ready_positive_smoke` coverage because the observed paths are expected to remain present.

`scenario_matrix.jsonl` is the coverage checklist for promoting new cases. Keep at least one positive and one negative case for symbol lookup, API flow, data-source tracing, config lookup, error/root-cause, cross-repo flow, and follow-up context before calling a release broadly improved.

Use this before and after retrieval, prompt, model, or indexing changes. The goal is to improve whole classes of source-code questions without tuning for one-off examples.

For a repeatable local quality check, run:

```bash
PYTHONPATH=. ./.venv/bin/python scripts/run_source_code_qa_nightly_eval.py --include-useful-feedback
```

The job writes timestamped reports under `TEAM_PORTAL_DATA_DIR/source_code_qa/eval_runs/` plus `latest.json`. It runs the deterministic fixture evals and regenerates feedback candidates, so it can be launched manually or scheduled by the host without adding a new portal workflow.

Use `--fixture` when you want a deterministic miniature repo set for regression checks. It creates AF and CRMS fixture repositories under the selected data root, then runs the same eval cases against generated code instead of depending on whatever repos happen to be synced locally.

The JSON output includes LLM routing and quality metadata (`llm_provider`, `llm_model`, `llm_route`, `llm_budget_mode`, `answer_claim_check`, and `answer_contract`) so regressions can be grouped by provider, budget, and evidence policy rather than inspected case by case.

Eval summaries also include `failure_buckets` (`retrieval`, `answer_policy`, `answer_content`, `query_status`, or `other`), `coverage_buckets`, and per-case `answer_policies`. Use these buckets to decide whether a failure needs retrieval/index work, policy tuning, answer rendering changes, or broader scenario coverage.

LLM and semantic retrieval can be switched without changing eval cases. `SOURCE_CODE_QA_LLM_PROVIDER=gemini` keeps the default Gemini route, while `SOURCE_CODE_QA_LLM_PROVIDER=openai_compatible` uses OpenAI-compatible `/chat/completions` settings. Semantic retrieval defaults to `local-token-hybrid-v1`; set `SOURCE_CODE_QA_EMBEDDING_PROVIDER=openai_compatible` and a non-local `SOURCE_CODE_QA_EMBEDDING_MODEL` to persist real embedding vectors in the chunk index.

The fixture now contains Java, Python, TypeScript, and two AF repos so the eval runner can validate Tree-sitter parser coverage, symbol edges, Feign/service-name matching, and HTTP-path cross-repo graph edges deterministically.

The scenario matrix also includes Chinese business-language prompts for data-source, impact, test-coverage, and operational-boundary questions. Keep these multilingual cases green so business users can ask naturally without knowing the English retrieval vocabulary.
