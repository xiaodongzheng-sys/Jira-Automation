# Source Code Q&A evals

This folder holds golden retrieval cases for the Source Code Q&A product.

Each JSONL row is one user question plus objective checks:

```json
{"id":"af-bpmis-batch-create","pm_team":"AF","country":"All","question":"where is batchCreateJiraIssue implemented","expected_paths":["bpmis/jira_client.py"],"required_terms":["batchCreateJiraIssue"]}
```

Run against the synced portal data:

```bash
PYTHONPATH=. ./.venv/bin/python scripts/run_source_code_qa_evals.py --cases evals/source_code_qa/golden.jsonl
```

Turn user feedback into draft eval candidates:

```bash
python3 scripts/source_code_qa_feedback_to_eval.py --output evals/source_code_qa/feedback_candidates.jsonl
```

Review the generated candidates before promoting them into `golden.jsonl`. Negative feedback is included by default; add `--include-useful` if you also want positive smoke-test cases.

`scenario_matrix.jsonl` is the coverage checklist for promoting new cases. Keep at least one positive and one negative case for symbol lookup, API flow, data-source tracing, config lookup, error/root-cause, cross-repo flow, and follow-up context before calling a release broadly improved.

Use this before and after retrieval, prompt, model, or indexing changes. The goal is to improve whole classes of source-code questions without tuning for one-off examples.
