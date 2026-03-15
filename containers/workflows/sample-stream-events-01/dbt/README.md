Workflow-owned dbt project for `sample-stream-events-01`.

This dbt runtime is intentionally owned by the workflow rather than the shared
ELT core. Different workflows are expected to diverge in model shape, tests,
and published marts even when they share the same ingest and standardize
runtimes.

For this sample, dbt:
- creates an external Athena source table over Firehose-delivered JSON objects
- stages and deduplicates sensor events by `emitter_event_id`
- publishes analytics marts for raw events, latest device state, and daily site metrics
