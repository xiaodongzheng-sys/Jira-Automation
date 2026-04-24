# Source Code Q&A evals

This folder holds golden retrieval cases for the Source Code Q&A product.

Each JSONL row is one user question plus objective checks:

```json
{"id":"af-bpmis-batch-create","pm_team":"AF","country":"All","question":"where is batchCreateJiraIssue implemented","expected_paths":["bpmis/jira_client.py"],"required_terms":["batchCreateJiraIssue"]}
```

Run against the synced portal data:

```bash
python3 scripts/run_source_code_qa_evals.py --cases evals/source_code_qa/golden.jsonl
```

Use this before and after retrieval, prompt, model, or indexing changes. The goal is to improve whole classes of source-code questions without tuning for one-off examples.
