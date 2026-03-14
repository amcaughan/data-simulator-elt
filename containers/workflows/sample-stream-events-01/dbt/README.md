Workflow-owned dbt project for `sample-stream-events-01`.

This dbt runtime is intentionally owned by the workflow rather than the shared
ELT core. Different workflows are expected to diverge in model shape, tests,
and published marts even when they share the same ingest and standardize
runtimes.
