Scheduled file-delivery workflow.

Intent:
- treat one API response as one client batch delivery
- preserve source-system and delivery metadata in the landing layer
- land two delivered CSV files per slice as `location_1.csv` and `location_2.csv`
- standardize landing files into day-level Parquet under `processed/raw`
- build workflow-local dbt models for canonical bundle selection, combined location records, and delivery marts

This workflow owns its dbt project under `containers/workflows/sample-file-delivery-01/dbt/`.
It currently uses the simulator API's `batch_delivery_benchmark` preset.

Its workflow-local dbt image is published with:

```bash
./scripts/release/workflow-images.sh --env dev --workflow sample-file-delivery-01
```

Manual examples:

```bash
./scripts/run/scheduled-workflow.sh \
  --workflow sample-file-delivery-01
```

```bash
./scripts/run/scheduled-workflow.sh \
  --workflow sample-file-delivery-01 \
  --step source-ingest \
  --planning-mode temporal \
  --slice-selector-mode range \
  --slice-range-start-at 2026-03-01T00:00:00Z \
  --slice-range-end-at 2026-03-07T23:59:59Z \
  --wait
```

The workflow-local dbt layer expects `standardize` to have already produced raw
Parquet under `processed/raw`. It then materializes:
- bronze canonical raw-bundle selection plus canonical delivered records
- silver combined location-level records and delivery-batch summaries
- gold daily and per-location delivery aggregates
- marts for analytics and management reporting
