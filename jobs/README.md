# jobs

Executable runtime source trees live here.

These are the shared platform runtimes. The ELT core stack builds immutable
images from these directories through the shared `container-image` module in
`infra/terragrunt/modules/`, then publishes image URIs for workflow stacks to
consume.

Each runtime directory should be self-contained enough to publish as an image:
- `Dockerfile`
- `requirements.in`
- `requirements.txt`
- runtime code and any local scaffolding

The shared runtime split is:
- `source_ingest/`
  source-facing landing ingestion with generic slice orchestration and source adapters
- `standardize/`
  landing-to-processed normalization into Parquet with source-specific parsers
- `stream_emitter/`
  upstream source simulation for stream-oriented workflows

Workflow-specific dbt projects do not live here. They live under:
- `workflows/<workflow_name>/dbt/`

That keeps the shared platform runtimes separate from workflow-owned
transformation logic.

The source-ingest runtime is intentionally adapter-driven:
- generic date/backfill orchestration
- exact landing writes
- source-specific fetch logic in adapters

The standardize runtime follows the same pattern:
- generic slice selection and Parquet write behavior
- source-specific landing parsers under `standardize/parsers/`
