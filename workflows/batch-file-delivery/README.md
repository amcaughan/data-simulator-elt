Scheduled file-delivery workflow.

Intent:
- treat one API response as one client batch delivery
- preserve source-system and delivery metadata in the landing layer
- standardize landing files into day-level Parquet under `processed/raw`
- later support downstream models for file-drop style ingestion and normalization

This workflow owns its dbt project under `workflows/batch-file-delivery/dbt/`.
