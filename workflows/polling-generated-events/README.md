Frequent scheduled polling workflow.

Intent:
- repeatedly pull generated benchmark data from the simulator API
- land exact source responses into the landing bucket
- standardize landing files into day-level Parquet under `processed/bronze`
- build workflow-local dbt silver, gold, and mart tables from `processed/bronze`

This workflow owns its dbt project under `containers/workflows/polling-generated-events/dbt/`.

Manual examples:

```bash
./scripts/run-scheduled-workflow.sh \
  --workflow polling-generated-events
```

```bash
./scripts/run-scheduled-workflow.sh \
  --workflow polling-generated-events \
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
bronze parquet under `processed/bronze`. It bootstraps an Athena external table
over that prefix, then materializes:
- silver transaction and answer-key tables
- gold row-level and daily aggregate tables
- marts for analytics, model evaluation, and management reporting
