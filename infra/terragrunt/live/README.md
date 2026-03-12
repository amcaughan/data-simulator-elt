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
- workflow runtime definitions
- workflow-specific scheduling or stream resources

These stacks assume the shared network layer eventually provides the private
service endpoints needed for ECS task startup and logging.

Current environment layout:
- `dev/core`
- `dev/batch-transactions`
- `dev/batch-batch-delivery`
- `dev/stream-iot`
- `prod/core`
- `prod/batch-transactions`
- `prod/batch-batch-delivery`
- `prod/stream-iot`
