# live

Deployable Terragrunt stacks live here.

Each environment is split into:
- `core/`
  shared ELT control-plane resources for that environment
- one stack per isolated workflow

Current workflow families:
- scheduled batch-style ingestion
- streaming ingestion with a workflow-owned simulated upstream producer

Each workflow stack is expected to compose:
- isolated storage
- source ingest
- standardize
- optionally dbt
- workflow-specific scheduling or stream resources

For the streaming sample, the schedules may be intentionally `null` so the
stack can be applied without constantly emitting demo data. One-off runs can
still be launched with `scripts/run-streaming-workflow.sh`.

These stacks assume the shared network layer eventually provides the private
service endpoints needed for ECS task startup and logging.

Current environment layout:
- `dev/core`
- `dev/sample-api-polling-01`
- `dev/sample-file-delivery-01`
- `dev/sample-stream-events-01`
- `prod/core`
- `prod/sample-api-polling-01`
- `prod/sample-file-delivery-01`
- `prod/sample-stream-events-01`
