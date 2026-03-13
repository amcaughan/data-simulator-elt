Workflow-owned dbt project for `polling-generated-events`.

This dbt runtime is intentionally owned by the workflow rather than the shared
ELT core. Different workflows are expected to diverge in model shape, tests,
and published marts even when they share the same ingest and standardize
runtimes.

This workflow treats `processed/bronze` as its raw dbt source layer:
- bronze parquet is produced by the shared `standardize` container
- dbt bootstraps an Athena external table over that bronze prefix at run start
- silver models split transaction facts from anomaly answer-key data
- gold models add derived row-level fields plus daily aggregates
- mart models publish audience-specific outputs into the marts bucket
