Workflow-owned dbt project for `batch-file-delivery`.

This dbt runtime is intentionally owned by the workflow rather than the shared
ELT core. Different workflows are expected to diverge in model shape, tests,
and published marts even when they share the same ingest and standardize
runtimes.
