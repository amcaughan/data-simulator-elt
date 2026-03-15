# models

dbt models for this workflow are organized by layer:
- `raw/`
- `staging/`
- `intermediate/`
- `marts/`

`raw/` holds the source declaration for Firehose-delivered newline-delimited
JSON events in the workflow processed bucket.

`staging/` parses and types those stream events into a clean event table.

`intermediate/` deduplicates events by `emitter_event_id` and acts as the
canonical event grain for the workflow.

`marts/` publishes analytics-friendly outputs such as raw event facts, latest
device status, and daily site-level metrics.
