# models

dbt models for this workflow are organized by warehouse layer:
- `bronze/`
- `silver/`
- `gold/`
- `marts/`

`bronze/` holds source declarations for the standardized parquet already written
to `processed/bronze`.

`silver/` keeps minimally cleaned row-grain models, including the split between
transaction facts and anomaly answer-key metadata.

`gold/` adds derived columns and aggregate tables that act as the canonical
warehouse outputs for this workflow.

`marts/` publishes audience-specific subsets of the gold layer into the marts
bucket.
