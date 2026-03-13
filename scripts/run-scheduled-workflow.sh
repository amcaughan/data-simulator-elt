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
  --planning-mode NAME         temporal | manual. Default: temporal
  --slice-selector-mode NAME   current | pinned | range | relative.
  --slice-pinned-at ISO        Pinned slice anchor for selector mode pinned.
  --slice-range-start-at ISO   Range start for selector mode range.
  --slice-range-end-at ISO     Range end for selector mode range.
  --slice-relative-count N     Slice count for selector mode relative.
  --slice-relative-direction D backward | forward. Default: backward
  --slice-relative-anchor-at I Optional anchor timestamp for selector mode relative.
  --slice-alignment NAME       floor | ceil | strict. Default: floor
  --slice-range-policy NAME    overlap | contained | strict. Default: overlap
  --adapter-config-json JSON   Override SOURCE_ADAPTER_CONFIG_JSON for the run.
  --adapter-config-file PATH   Read adapter config override from a file.
  --manual-request-json JSON   Manual request payload for source-ingest manual mode.
  --manual-request-file PATH   Read manual request payload from a file.
  --manual-storage-prefix P    Manual storage prefix for source-ingest manual mode.
  --manual-object-name NAME    Optional manual object name override.
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
    --slice-selector-mode relative \
    --slice-relative-count 7

  ./scripts/run-scheduled-workflow.sh \
    --workflow batch-file-delivery \
    --step source-ingest \
    --slice-selector-mode pinned \
    --slice-pinned-at 2026-03-01
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENVIRONMENT="dev"
WORKFLOW_NAME=""
STEP="both"
PLANNING_MODE="temporal"
SLICE_SELECTOR_MODE=""
SLICE_PINNED_AT=""
SLICE_RANGE_START_AT=""
SLICE_RANGE_END_AT=""
SLICE_RELATIVE_COUNT=""
SLICE_RELATIVE_DIRECTION=""
SLICE_RELATIVE_ANCHOR_AT=""
SLICE_ALIGNMENT_POLICY=""
SLICE_RANGE_POLICY=""
ADAPTER_CONFIG_JSON=""
ADAPTER_CONFIG_FILE=""
MANUAL_REQUEST_JSON=""
MANUAL_REQUEST_FILE=""
MANUAL_STORAGE_PREFIX=""
MANUAL_OBJECT_NAME=""
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
    --planning-mode)
      PLANNING_MODE="${2:-}"
      shift 2
      ;;
    --slice-selector-mode)
      SLICE_SELECTOR_MODE="${2:-}"
      shift 2
      ;;
    --slice-pinned-at)
      SLICE_PINNED_AT="${2:-}"
      shift 2
      ;;
    --slice-range-start-at)
      SLICE_RANGE_START_AT="${2:-}"
      shift 2
      ;;
    --slice-range-end-at)
      SLICE_RANGE_END_AT="${2:-}"
      shift 2
      ;;
    --slice-relative-count)
      SLICE_RELATIVE_COUNT="${2:-}"
      shift 2
      ;;
    --slice-relative-direction)
      SLICE_RELATIVE_DIRECTION="${2:-}"
      shift 2
      ;;
    --slice-relative-anchor-at)
      SLICE_RELATIVE_ANCHOR_AT="${2:-}"
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
    --manual-request-json)
      MANUAL_REQUEST_JSON="${2:-}"
      shift 2
      ;;
    --manual-request-file)
      MANUAL_REQUEST_FILE="${2:-}"
      shift 2
      ;;
    --manual-storage-prefix)
      MANUAL_STORAGE_PREFIX="${2:-}"
      shift 2
      ;;
    --manual-object-name)
      MANUAL_OBJECT_NAME="${2:-}"
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

if [[ -n "$MANUAL_REQUEST_JSON" && -n "$MANUAL_REQUEST_FILE" ]]; then
  echo "Use only one of --manual-request-json or --manual-request-file" >&2
  exit 1
fi

if [[ -n "$ADAPTER_CONFIG_FILE" ]]; then
  ADAPTER_CONFIG_JSON="$(cat "$ADAPTER_CONFIG_FILE")"
fi

if [[ -n "$MANUAL_REQUEST_FILE" ]]; then
  MANUAL_REQUEST_JSON="$(cat "$MANUAL_REQUEST_FILE")"
fi

case "$PLANNING_MODE" in
  temporal|manual) ;;
  *)
    echo "--planning-mode must be one of: temporal, manual" >&2
    exit 1
    ;;
esac

if [[ "$PLANNING_MODE" == "manual" && "$STEP" != "source-ingest" ]]; then
  echo "--planning-mode manual only supports --step source-ingest" >&2
  exit 1
fi

if [[ "$PLANNING_MODE" == "temporal" && -z "$SLICE_SELECTOR_MODE" ]]; then
  if [[ -n "$SLICE_RANGE_START_AT" || -n "$SLICE_RANGE_END_AT" ]]; then
    SLICE_SELECTOR_MODE="range"
  elif [[ -n "$SLICE_RELATIVE_COUNT" ]]; then
    SLICE_SELECTOR_MODE="relative"
  elif [[ -n "$SLICE_PINNED_AT" ]]; then
    SLICE_SELECTOR_MODE="pinned"
  else
    SLICE_SELECTOR_MODE="current"
  fi
fi

if [[ "$PLANNING_MODE" == "temporal" && "$SLICE_SELECTOR_MODE" == "relative" && -z "$SLICE_RELATIVE_DIRECTION" ]]; then
  SLICE_RELATIVE_DIRECTION="backward"
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
    PLANNING_MODE="$PLANNING_MODE" \
    SLICE_SELECTOR_MODE="$SLICE_SELECTOR_MODE" \
    SLICE_PINNED_AT="$SLICE_PINNED_AT" \
    SLICE_RANGE_START_AT="$SLICE_RANGE_START_AT" \
    SLICE_RANGE_END_AT="$SLICE_RANGE_END_AT" \
    SLICE_RELATIVE_COUNT="$SLICE_RELATIVE_COUNT" \
    SLICE_RELATIVE_DIRECTION="$SLICE_RELATIVE_DIRECTION" \
    SLICE_RELATIVE_ANCHOR_AT="$SLICE_RELATIVE_ANCHOR_AT" \
    SLICE_ALIGNMENT_POLICY="$SLICE_ALIGNMENT_POLICY" \
    SLICE_RANGE_POLICY="$SLICE_RANGE_POLICY" \
    ADAPTER_CONFIG_JSON="$ADAPTER_CONFIG_JSON" \
    MANUAL_REQUEST_JSON="$MANUAL_REQUEST_JSON" \
    MANUAL_STORAGE_PREFIX="$MANUAL_STORAGE_PREFIX" \
    MANUAL_OBJECT_NAME="$MANUAL_OBJECT_NAME" \
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
planning_mode = os.environ.get("PLANNING_MODE", "temporal")

env = []

if step_name == "source-ingest":
    env.append({"name": "PLANNING_MODE", "value": planning_mode})

if planning_mode == "manual":
    manual_request_json = os.environ.get("MANUAL_REQUEST_JSON")
    if manual_request_json:
        env.append({"name": "MANUAL_REQUEST_JSON", "value": manual_request_json})
    for key in ("MANUAL_STORAGE_PREFIX", "MANUAL_OBJECT_NAME"):
        value = os.environ.get(key)
        if value:
            env.append({"name": key, "value": value})
else:
    env.append({"name": "SLICE_SELECTOR_MODE", "value": os.environ["SLICE_SELECTOR_MODE"]})

    for key in (
        "SLICE_PINNED_AT",
        "SLICE_RANGE_START_AT",
        "SLICE_RANGE_END_AT",
        "SLICE_RELATIVE_COUNT",
        "SLICE_RELATIVE_DIRECTION",
        "SLICE_RELATIVE_ANCHOR_AT",
    ):
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

if planning_mode == "temporal":
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
