# containers

All image build inputs live here.

This directory is the packaging entrypoint for the repository. The release
scripts build and publish immutable images from these source trees into the ECR
repositories created by Terragrunt.

The split is:
- `shared/`
  shared platform runtimes reused across workflows
- `workflows/`
  workflow-owned runtime sources such as dbt projects and simulated upstream producers

Shared runtimes include:
- `source_ingest/`
- `standardize/`
- `common/`

Workflow-owned runtimes currently include:
- `containers/workflows/<workflow_name>/dbt/`
- simulated upstream emitters when a workflow needs its own fake producer

The repository still uses `workflows/` at the top level for workload intent,
docs, and configuration. `containers/` exists to make packaging and image
ownership obvious at a glance.
