# modules

Terraform modules local to this repository live here.

The current split is:
- `elt-core`
  shared control-plane resources for one environment
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
- workflow modules compose storage, runtime modules, workflow-local ECR
  repositories, and orchestration
- live stacks instantiate concrete example workloads

Image publishing now happens outside Terraform through:
- `scripts/release-core-images.sh`
- `scripts/release-workflow-images.sh`

Those scripts write local release manifests under `build/releases/<env>/`, and
the live Terragrunt stacks read those manifests when wiring task definitions to
immutable image URIs.
