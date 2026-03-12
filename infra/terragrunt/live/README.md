# live

Deployable Terragrunt stacks live here.

Each environment is split into:
- `core/`
  shared ELT control-plane resources for that environment
- one stack per isolated workflow

Current workflow families:
- scheduled batch-style ingestion
- stream-emitter style ingestion

Each workflow stack is expected to compose:
- isolated storage
- source ingest
- standardize
- optionally dbt
- workflow-specific scheduling or stream resources

These stacks assume the shared network layer eventually provides the private
service endpoints needed for ECS task startup and logging.

Current environment layout:
- `dev/core`
- `dev/polling-generated-events`
- `dev/batch-file-delivery`
- `dev/stream-sampled-events`
- `prod/core`
- `prod/polling-generated-events`
- `prod/batch-file-delivery`
- `prod/stream-sampled-events`
