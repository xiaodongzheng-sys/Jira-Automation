# Source Code QA Domain Knowledge Packs

The domain knowledge packs live in `config/source_code_qa_domain_knowledge_packs.json`.

They are local-only context for Source Code Q&A. They do not add online data, change access control, or switch LLM models. The service loads them at runtime and merges each team's `retrieval_terms` into the existing domain profile before query decomposition and retrieval.

## What Each Pack Contains

- `module_map`: business modules, aliases, likely repos, code hints, and business flows.
- `terminology`: business terms mapped to Chinese/English aliases and code terms.
- `key_artifacts`: table, API, and config-name hints for code evidence discovery.
- `retrieval_terms`: machine-usable terms merged into `data_carriers`, `source_terms`, `api_terms`, `config_terms`, `logic_terms`, and `field_population_terms`.
- `evidence_rules`: domain-specific answer discipline, such as not treating DTOs as final data sources.
- `question_seeds`: typical questions that can be promoted into eval cases after review.

## Current Coverage

- `CRMS`: underwriting initiation, term-loan precheck, customer/loan/credit info, and feature data.
- `AF`: black/white list, case review, flow report, CRC record, and risk config/strategy.
- `GRC`: parameter management, authorization management, approval flow, and global lock.

## Update Workflow

1. Add new business aliases, tables, API paths, config keys, or module hints to the relevant domain.
2. Add or update `retrieval_terms` when the hint should actively influence search.
3. Add representative `question_seeds` for recurring questions.
4. Run:

```bash
PYTHONPATH=. ./.venv/bin/python -m unittest discover -s tests -p 'test_source_code_qa.py'
PYTHONPATH=. ./.venv/bin/python scripts/run_source_code_qa_release_gate.py --include-useful-feedback
```
