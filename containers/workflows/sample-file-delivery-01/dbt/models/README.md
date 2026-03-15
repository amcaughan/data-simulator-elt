# models

dbt models for this workflow are organized by warehouse layer:
- `raw/`
- `bronze/`
- `silver/`
- `gold/`
- `marts/`

`raw/` holds source declarations for the standardized delivery Parquet already
written to `processed/raw`.

`bronze/` picks one canonical raw delivery bundle per slice and reads canonical
row-level delivery records from that bundle.

`silver/` combines both delivered location files into cleaner semantic tables
for row-level records and delivery-batch summaries.

`gold/` adds daily and per-location aggregates that act as the canonical
analytical outputs for this workflow.

`marts/` publishes audience-specific subsets of the gold layer into the marts
bucket.
