#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/run-scheduled-workflow.sh --workflow WORKFLOW [options]

Run one-off ECS tasks for a scheduled workflow stack without changing the
scheduler resources. This is useful for manual ingest runs and quick backfills.

Required:
  --workflow NAME              Workflow stack under infra/terragrunt/live/<env>/

Options:
  --env NAME                   Environment name. Default: dev
  --step NAME                  source-ingest | standardize | both. Default: both
  --mode NAME                  live_hit | backfill. Defaults to live_hit,
                               or backfill if a range/backfill flag is supplied.
  --logical-date ISO           Logical date for a one-off live hit.
  --start-at ISO               Backfill range start.
  --end-at ISO                 Backfill range end.
  --backfill-count N           Backfill the previous N logical slices.
  --slice-alignment NAME       floor | ceil | strict. Default: floor
  --slice-range-policy NAME    overlap | contained | strict. Default: overlap
  --adapter-config-json JSON   Override SOURCE_ADAPTER_CONFIG_JSON for the run.
  --adapter-config-file PATH   Read adapter config override from a file.
  --landing-base-prefix PATH   Override LANDING_BASE_PREFIX for the run.
  --landing-partitions JSON    Override LANDING_PARTITION_FIELDS_JSON for the run.
  --landing-path-suffix JSON   Override LANDING_PATH_SUFFIX_JSON for the run.
  --landing-input-prefix PATH  Override LANDING_INPUT_PREFIX for standardize.
  --processed-output-prefix P  Override PROCESSED_OUTPUT_PREFIX for standardize.
  --wait                       Wait for the final task to stop before exiting.
  --help                       Show this help.

Examples:
  ./scripts/run-scheduled-workflow.sh \
    --workflow polling-generated-events

  ./scripts/run-scheduled-workflow.sh \
    --workflow polling-generated-events \
    --mode backfill \
    --backfill-count 7

  ./scripts/run-scheduled-workflow.sh \
    --workflow batch-file-delivery \
    --step source-ingest \
    --logical-date 2026-03-01
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENVIRONMENT="dev"
WORKFLOW_NAME=""
STEP="both"
MODE=""
LOGICAL_DATE=""
START_AT=""
END_AT=""
BACKFILL_COUNT=""
SLICE_ALIGNMENT_POLICY=""
SLICE_RANGE_POLICY=""
ADAPTER_CONFIG_JSON=""
ADAPTER_CONFIG_FILE=""
LANDING_BASE_PREFIX=""
LANDING_PARTITIONS_JSON=""
LANDING_PATH_SUFFIX_JSON=""
LANDING_INPUT_PREFIX=""
PROCESSED_OUTPUT_PREFIX=""
WAIT_FOR_FINAL="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workflow)
      WORKFLOW_NAME="${2:-}"
      shift 2
      ;;
    --env)
      ENVIRONMENT="${2:-}"
      shift 2
      ;;
    --step)
      STEP="${2:-}"
      shift 2
      ;;
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --logical-date)
      LOGICAL_DATE="${2:-}"
      shift 2
      ;;
    --start-at)
      START_AT="${2:-}"
      shift 2
      ;;
    --end-at)
      END_AT="${2:-}"
      shift 2
      ;;
    --backfill-count)
      BACKFILL_COUNT="${2:-}"
      shift 2
      ;;
    --slice-alignment)
      SLICE_ALIGNMENT_POLICY="${2:-}"
      shift 2
      ;;
    --slice-range-policy)
      SLICE_RANGE_POLICY="${2:-}"
      shift 2
      ;;
    --adapter-config-json)
      ADAPTER_CONFIG_JSON="${2:-}"
      shift 2
      ;;
    --adapter-config-file)
      ADAPTER_CONFIG_FILE="${2:-}"
      shift 2
      ;;
    --landing-base-prefix)
      LANDING_BASE_PREFIX="${2:-}"
      shift 2
      ;;
    --landing-partitions)
      LANDING_PARTITIONS_JSON="${2:-}"
      shift 2
      ;;
    --landing-path-suffix)
      LANDING_PATH_SUFFIX_JSON="${2:-}"
      shift 2
      ;;
    --landing-input-prefix)
      LANDING_INPUT_PREFIX="${2:-}"
      shift 2
      ;;
    --processed-output-prefix)
      PROCESSED_OUTPUT_PREFIX="${2:-}"
      shift 2
      ;;
    --wait)
      WAIT_FOR_FINAL="true"
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$WORKFLOW_NAME" ]]; then
  echo "--workflow is required" >&2
  usage >&2
  exit 1
fi

case "$STEP" in
  source-ingest|standardize|both) ;;
  *)
    echo "--step must be one of: source-ingest, standardize, both" >&2
    exit 1
    ;;
esac

if [[ -n "$ADAPTER_CONFIG_JSON" && -n "$ADAPTER_CONFIG_FILE" ]]; then
  echo "Use only one of --adapter-config-json or --adapter-config-file" >&2
  exit 1
fi

if [[ -n "$ADAPTER_CONFIG_FILE" ]]; then
  ADAPTER_CONFIG_JSON="$(cat "$ADAPTER_CONFIG_FILE")"
fi

if [[ -z "$MODE" ]]; then
  if [[ -n "$START_AT" || -n "$END_AT" || -n "$BACKFILL_COUNT" ]]; then
    MODE="backfill"
  else
    MODE="live_hit"
  fi
fi

STACK_DIR="${REPO_ROOT}/infra/terragrunt/live/${ENVIRONMENT}/${WORKFLOW_NAME}"
if [[ ! -d "$STACK_DIR" ]]; then
  echo "Workflow stack not found: ${STACK_DIR}" >&2
  exit 1
fi

OUTPUTS_FILE="$(mktemp)"
trap 'rm -f "$OUTPUTS_FILE"' EXIT

(
  cd "$STACK_DIR"
  terragrunt output -json > "$OUTPUTS_FILE"
)

run_step() {
  local step_name="$1"
  local container_name task_output_name

  case "$step_name" in
    source-ingest)
      container_name="source-ingest"
      task_output_name="source_ingest_task_definition_arn"
      ;;
    standardize)
      container_name="standardize"
      task_output_name="standardize_task_definition_arn"
      ;;
    *)
      echo "Unsupported step: $step_name" >&2
      exit 1
      ;;
  esac

  local cli_input
  cli_input="$(
    OUTPUTS_FILE="$OUTPUTS_FILE" \
    AWS_REGION_OVERRIDE="${AWS_REGION:-}" \
    STEP_NAME="$step_name" \
    TASK_OUTPUT_NAME="$task_output_name" \
    CONTAINER_NAME="$container_name" \
    MODE="$MODE" \
    LOGICAL_DATE="$LOGICAL_DATE" \
    START_AT="$START_AT" \
    END_AT="$END_AT" \
    BACKFILL_COUNT="$BACKFILL_COUNT" \
    SLICE_ALIGNMENT_POLICY="$SLICE_ALIGNMENT_POLICY" \
    SLICE_RANGE_POLICY="$SLICE_RANGE_POLICY" \
    ADAPTER_CONFIG_JSON="$ADAPTER_CONFIG_JSON" \
    LANDING_BASE_PREFIX="$LANDING_BASE_PREFIX" \
    LANDING_PARTITIONS_JSON="$LANDING_PARTITIONS_JSON" \
    LANDING_PATH_SUFFIX_JSON="$LANDING_PATH_SUFFIX_JSON" \
    LANDING_INPUT_PREFIX="$LANDING_INPUT_PREFIX" \
    PROCESSED_OUTPUT_PREFIX="$PROCESSED_OUTPUT_PREFIX" \
    WORKFLOW_NAME="$WORKFLOW_NAME" \
    python3 - <<'PY'
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

outputs = json.loads(Path(os.environ["OUTPUTS_FILE"]).read_text())

def output_value(name):
    try:
        return outputs[name]["value"]
    except KeyError as exc:
        raise SystemExit(f"Missing workflow output: {name}") from exc

task_definition = output_value(os.environ["TASK_OUTPUT_NAME"])
cluster_arn = output_value("ecs_cluster_arn")
subnets = output_value("network_private_subnet_ids")
security_group = output_value("network_security_group_id")
aws_region = os.environ.get("AWS_REGION_OVERRIDE") or output_value("aws_region")
step_name = os.environ["STEP_NAME"]
container_name = os.environ["CONTAINER_NAME"]

env = [{"name": "MODE", "value": os.environ["MODE"]}]

for key in ("LOGICAL_DATE", "START_AT", "END_AT", "BACKFILL_COUNT"):
    value = os.environ.get(key)
    if value:
        env.append({"name": key, "value": value})

for key in ("SLICE_ALIGNMENT_POLICY", "SLICE_RANGE_POLICY"):
    value = os.environ.get(key)
    if value:
        env.append({"name": key, "value": value})

adapter_config_json = os.environ.get("ADAPTER_CONFIG_JSON")
if adapter_config_json:
    env.append(
        {
            "name": "SOURCE_ADAPTER_CONFIG_JSON",
            "value": adapter_config_json,
        }
    )

for env_name in (
    "LANDING_BASE_PREFIX",
    "LANDING_PARTITION_FIELDS_JSON",
    "LANDING_PATH_SUFFIX_JSON",
):
    if env_name == "LANDING_PARTITION_FIELDS_JSON":
        value = os.environ.get("LANDING_PARTITIONS_JSON")
    else:
        value = os.environ.get(env_name)
    if value:
        env.append({"name": env_name, "value": value})

if step_name == "standardize":
    landing_input_prefix = os.environ.get("LANDING_INPUT_PREFIX")
    if landing_input_prefix:
        env.append(
            {
                "name": "LANDING_INPUT_PREFIX",
                "value": landing_input_prefix,
            }
        )

    processed_output_prefix = os.environ.get("PROCESSED_OUTPUT_PREFIX")
    if processed_output_prefix:
        env.append(
            {
                "name": "PROCESSED_OUTPUT_PREFIX",
                "value": processed_output_prefix,
            }
        )

started_by = f"manual-{os.environ['WORKFLOW_NAME']}-{step_name}"
created_on = datetime.now(UTC).date().isoformat()
payload = {
    "cluster": cluster_arn,
    "launchType": "FARGATE",
    "taskDefinition": task_definition,
    "startedBy": started_by,
    "tags": [
        {"key": "auto_cleanup", "value": "true"},
        {"key": "cleanup_schedule", "value": "daily"},
        {"key": "created_on", "value": created_on},
    ],
    "networkConfiguration": {
        "awsvpcConfiguration": {
            "subnets": subnets,
            "securityGroups": [security_group],
            "assignPublicIp": "DISABLED",
        }
    },
    "overrides": {
        "containerOverrides": [
            {
                "name": container_name,
                "environment": env,
            }
        ]
    },
}

sys.stdout.write(json.dumps({"region": aws_region, "payload": payload}))
PY
  )"

  local aws_region task_arn
  aws_region="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["region"])' <<<"$cli_input")"
  task_arn="$(
    python3 -c 'import json,sys; print(json.dumps(json.loads(sys.stdin.read())["payload"]))' <<<"$cli_input" \
      | aws ecs run-task \
          --region "$aws_region" \
          --cli-input-json file:///dev/stdin \
          --query 'tasks[0].taskArn' \
          --output text
  )"

  if [[ "$task_arn" == "None" || -z "$task_arn" ]]; then
    echo "Failed to start ${step_name} task" >&2
    exit 1
  fi

  echo "Started ${step_name} task:"
  echo "  workflow: ${WORKFLOW_NAME}"
  echo "  env:      ${ENVIRONMENT}"
  echo "  task:     ${task_arn}"

  if [[ "$step_name" == "source-ingest" && "$STEP" == "both" ]]; then
    echo "Waiting for source-ingest to finish before starting standardize..."
    aws ecs wait tasks-stopped --region "$aws_region" --cluster "$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["payload"]["cluster"])' <<<"$cli_input")" --tasks "$task_arn"
  elif [[ "$WAIT_FOR_FINAL" == "true" ]]; then
    echo "Waiting for ${step_name} to stop..."
    aws ecs wait tasks-stopped --region "$aws_region" --cluster "$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["payload"]["cluster"])' <<<"$cli_input")" --tasks "$task_arn"
  fi
}

case "$STEP" in
  source-ingest)
    run_step "source-ingest"
    ;;
  standardize)
    run_step "standardize"
    ;;
  both)
    run_step "source-ingest"
    run_step "standardize"
    ;;
esac
