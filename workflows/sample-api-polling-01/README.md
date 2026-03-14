Frequent scheduled polling workflow.

Intent:
- repeatedly pull generated benchmark data from the simulator API
- land exact source responses into the landing bucket
- standardize landing files into day-level Parquet under `processed/raw`
- build workflow-local dbt bronze, silver, gold, and mart tables from `processed/raw`

This workflow owns its dbt project under `containers/workflows/sample-api-polling-01/dbt/`.

Manual examples:

```bash
./scripts/run-scheduled-workflow.sh \
  --workflow sample-api-polling-01
```

```bash
./scripts/run-scheduled-workflow.sh \
  --workflow sample-api-polling-01 \
  --step source-ingest \
  --planning-mode temporal \
  --slice-selector-mode range \
  --slice-range-start-at 2026-03-01T00:00:00Z \
  --slice-range-end-at 2026-03-03T23:59:59Z \
  --wait
```

This workflow currently uses `slice_granularity = "hour"`, so a three-day sample
backfill means three days of hourly slices rather than three daily pulls.

The workflow-local dbt image now expects `standardize` to have already produced
raw parquet under `processed/raw`. It bootstraps an Athena external table
over that prefix, then materializes:
- silver transaction and answer-key tables
- gold row-level and daily aggregate tables
- marts for analytics, model evaluation, and management reporting
