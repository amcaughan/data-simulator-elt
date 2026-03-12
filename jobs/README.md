# jobs

Reusable containerized runtime code lives here.

The initial job split is:
- `scheduled_ingest/`
  scheduled pull-based ingestion from the private simulator API
- `stream_emitter/`
  event-emitting runtime for stream-oriented workflows

Direct Python dependencies for these jobs should be managed through:
- `requirements.in`
- `requirements.txt`
