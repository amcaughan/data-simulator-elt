# jobs

Executable runtime source trees live here.

This directory is the canonical source for the shared `container-image` module
in `infra/terragrunt/modules/`. The ELT core stack builds immutable runtime
images from these directories and publishes image URIs for workflow stacks to
consume.

The initial job split is:
- `source_ingest/`
  source-facing landing ingestion with generic slice orchestration and source adapters
- `standardize/`
  landing-to-processed normalization into Parquet with source-specific parsers
- `dbt/`
  dbt project and runtime packaging for processed-to-marts transforms
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
