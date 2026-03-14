# models

dbt models for this workflow are organized by warehouse layer:
- `raw/`
- `bronze/`
- `silver/`
- `gold/`
- `marts/`

`raw/` holds source declarations for the standardized parquet already written to
`processed/raw`.

`bronze/` holds the first warehouse layer: canonical deduped event rows with
typed source fields and lineage.

`silver/` splits the bronze events into cleaner semantic row-grain models,
separating transaction facts from anomaly answer-key metadata.

`gold/` adds derived columns and aggregate tables that act as the canonical
warehouse outputs for this workflow.

`marts/` publishes audience-specific subsets of the gold layer into the marts
bucket.
