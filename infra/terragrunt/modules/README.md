# modules

Terraform modules local to this repository live here.

The current split is:
- `elt-core`
  shared control-plane resources for one environment
- `isolated-storage`
  per-workflow landing, raw, and analytics storage
- `scheduled-ingest-job`
  reusable runtime pattern for scheduled API ingestion
- `dbt-job`
  reusable runtime pattern for dbt-based transforms
- `scheduled-workflow`
  isolated scheduled workflow composition
- `streaming-job`
  reusable runtime pattern for event-emitting ingestion
- `streaming-workflow`
  isolated streaming workflow composition

The intended split is:
- job modules define reusable runtime and IAM patterns
- workflow modules compose storage, runtime modules, and orchestration
- live stacks instantiate concrete example workloads
