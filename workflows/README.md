Workload-level descriptions live here.

Each workflow should explain:
- what type of ingestion it demonstrates
- what isolation boundary it owns
- what simulator pattern or preset it uses as an example source
- what downstream transformations and marts it is meant to support

Each workflow may also own:
- a workflow-local `dbt/` project
- a workflow-specific dbt image

That is intentional. Shared source-ingest and standardize runtimes live under
`jobs/`, but transformation logic belongs to the workflow that owns the data
boundary.
