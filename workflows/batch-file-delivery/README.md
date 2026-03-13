Scheduled file-delivery workflow.

Intent:
- treat one API response as one client batch delivery
- preserve source-system and delivery metadata in the landing layer
- standardize landing files into day-level Parquet under `processed/raw`
- later support downstream models for file-drop style ingestion and normalization

This workflow owns its dbt project under `containers/workflows/batch-file-delivery/dbt/`.

Manual examples:

```bash
./scripts/run-scheduled-workflow.sh \
  --workflow batch-file-delivery
```

```bash
./scripts/run-scheduled-workflow.sh \
  --workflow batch-file-delivery \
  --step source-ingest \
  --planning-mode temporal \
  --slice-selector-mode range \
  --slice-range-start-at 2026-03-01T00:00:00Z \
  --slice-range-end-at 2026-03-07T23:59:59Z \
  --wait
```
