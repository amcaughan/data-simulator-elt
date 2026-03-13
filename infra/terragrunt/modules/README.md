# modules

Terraform modules local to this repository live here.

The current split is:
- `elt-core`
  shared control-plane resources for one environment
- `container-image`
  local Docker build-and-push helper for runtime images sourced from `containers/`
- `isolated-storage`
  per-workflow landing, processed, and marts storage
- `source-ingest-job`
  reusable runtime pattern for source landing ingestion
- `standardize-job`
  reusable runtime pattern for landing-to-processed standardization
- `dbt-job`
  reusable runtime pattern for dbt-based transforms
- `scheduled-workflow`
  isolated scheduled workflow composition
- `stream-emitter-job`
  reusable runtime pattern for simulating an upstream stream producer
- `streaming-workflow`
  isolated streaming workflow composition

The intended split is:
- job modules define reusable runtime and IAM patterns
- workflow modules compose storage, runtime modules, workflow-local dbt image publishing, and orchestration
- live stacks instantiate concrete example workloads
