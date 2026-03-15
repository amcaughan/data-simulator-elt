Workflow-owned dbt project for `sample-file-delivery-01`.

This dbt runtime is intentionally owned by the workflow rather than the shared
ELT core. Different workflows are expected to diverge in model shape, tests,
and published marts even when they share the same ingest and standardize
runtimes.

For this sample, dbt:
- bootstraps an Athena external table over standardized raw Parquet in `processed/raw`
- selects one canonical delivered bundle per slice before reading row-level data
- combines both delivered location files into shared silver models with explicit location labels
- publishes delivery analytics and management marts into the workflow marts bucket
