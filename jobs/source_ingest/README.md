Source-facing landing ingestion runtime.

Responsibilities:
- read the private simulator API URL from SSM
- sign requests with the task role and call the private simulator API
- write exact source payloads into the landing bucket
- support both single-run and backfill execution modes
- derive deterministic logical-date seeds when requested

Non-goals:
- no processed-data writes
- no Parquet standardization
- no dbt transformations

Those belong in later steps of the ELT flow.
