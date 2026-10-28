# Pipeline Focus Area and PRD Status

Pipeline columns are BPMIS Project ID, Pipeline Section, Priority, Focus Area,
PM and PRD Status, with the existing row-move control. Version bundle columns
are unchanged. Focus Area defaults to `No`. PRD Status defaults to `Not Started`
and accepts `Not Started`, `PRD In Progress` and `PRD Reviewed`. Historical Y/N
Productization Efforts values are retained but do not determine PRD Status.

Only existing Version Plan editors can change these fields. Focus Area and PRD
Status are Pipeline-only fields in the cell update API.

## Persistence and Lookup

`version_plan.af.pipeline_focus_areas` stores the last Focus Area for each BPMIS
ID independently of row membership. `pipeline_history` retains deleted rows,
previous ID associations and legacy duplicate rows. Neither is cleared by Jira
sync. There is no new page search control.

`GET /api/team-dashboard/version-plan/af/focus-area?bpmis_project_id=123456`
uses Version Plan read permissions. A successful response contains `status`,
`bpmis_project_id`, `focus_area`, `has_active_pipeline_row` and `updated_at`.
An active Pipeline row wins over saved history. Invalid/missing IDs return 400;
unknown IDs return 404. The ID must contain six ASCII digits.

Nonempty BPMIS IDs are unique within Pipeline. Creating a duplicate, changing an
ID to a duplicate, or moving a duplicate into Pipeline is rejected without
changing the original data. Legacy duplicate groups keep the greatest
`(updated_at, row_id)` after parsing timestamps; invalid/missing times sort first.
Other rows are archived in full. Repeated normalization does not duplicate history.

SQLite writes hold a transaction across revision validation and the plan write.
Firestore uses a transaction, or an updateTime precondition on the REST fallback.
The remote local-agent path forwards revision checks to the Mac transaction.
The Focus Area record, row and history are saved together. A failed remote write
must not overwrite a different local cache using the remote revision.

## Before Production Release

No production migration is needed for tests. Before deploying this change,
stop Version Plan edits/sync for the migration window, select the intended Live
store, and preview with:

```sh
./.venv/bin/python -m scripts.migrate_pipeline_focus_areas
```

Apply only with a new backup path outside the repository:

```sh
./.venv/bin/python -m scripts.migrate_pipeline_focus_areas \
  --apply --backup-file /absolute/private/path/version-plan-before-focus.json
```

The command writes the original, unnormalized source using mode 0600, flushes
the backup, and then saves with revision validation. Existing backup files are
never overwritten. Keep this backup private; it contains internal project data.
If the save conflicts, refresh and retry with a new backup file. Confirm counts,
restore lookups and repeat-preview idempotence before resuming edits/sync.

Daily Brief team-member follow-ups remain in email but no longer produce Trello
cards. Personal-action and Watch / Delegate cards are unchanged; old cards are
not deleted or moved by this change.
