# jobs

Reusable containerized runtime code lives here.

The initial job split is:
- `source_ingest/`
  source-facing landing ingestion from the private simulator API
- `stream_emitter/`
  upstream source simulation for stream-oriented workflows

Direct Python dependencies for these jobs should be managed through:
- `requirements.in`
- `requirements.txt`

The source-ingest runtime is intentionally adapter-driven:
- generic date/backfill orchestration
- exact landing writes
- source-specific fetch logic in adapters
