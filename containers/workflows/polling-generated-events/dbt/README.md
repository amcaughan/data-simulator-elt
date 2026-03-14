Workflow-owned dbt project for `polling-generated-events`.

This dbt runtime is intentionally owned by the workflow rather than the shared
ELT core. Different workflows are expected to diverge in model shape, tests,
and published marts even when they share the same ingest and standardize
runtimes.

This workflow treats `processed/raw` as its standardized dbt source layer:
- raw parquet is produced by the shared `standardize` container
- dbt bootstraps an Athena external table over that raw prefix at run start
- bronze models dedupe and canonicalize the standardized raw events
- silver models split transaction facts from anomaly answer-key data
- gold models add derived row-level fields plus daily aggregates
- mart models publish audience-specific outputs into the marts bucket

The runtime currently defaults to `dbt run` so the workflow can prove data flow
without blocking on test failures. Set `DBT_COMMAND=build` or `DBT_COMMAND=test`
explicitly when you want validation behavior.
