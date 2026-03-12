# jobs

Reusable containerized runtime code lives here.

The initial job split is:
- `source_ingest/`
  source-facing landing ingestion with generic slice orchestration and source adapters
- `standardize/`
  landing-to-processed normalization into Parquet with source-specific parsers
- `stream_emitter/`
  upstream source simulation for stream-oriented workflows

Direct Python dependencies for these jobs should be managed through:
- `requirements.in`
- `requirements.txt`

The source-ingest runtime is intentionally adapter-driven:
- generic date/backfill orchestration
- exact landing writes
- source-specific fetch logic in adapters

The standardize runtime follows the same pattern:
- generic slice selection and Parquet write behavior
- source-specific landing parsers under `standardize/parsers/`
