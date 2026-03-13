#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/run-scheduled-workflow.sh --workflow WORKFLOW [options]

Run one-off ECS tasks for a scheduled workflow stack without changing the
scheduler resources. This resolves workflow outputs, then delegates to the
lower-level ECS step runner.

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
  --source-adapter-config-json JSON   Override SOURCE_ADAPTER_CONFIG_JSON for source-ingest.
  --source-adapter-config-file PATH   Read source adapter config override from a file.
  --standardize-strategy-config-json JSON   Override STANDARDIZE_STRATEGY_CONFIG_JSON for standardize.
  --standardize-strategy-config-file PATH   Read standardize strategy config override from a file.
  --manual-request-json JSON   Manual request payload for source-ingest manual mode.
  --manual-request-file PATH   Read manual request payload from a file.
  --manual-storage-prefix P    Manual storage prefix for source-ingest manual mode.
  --manual-input-prefix P      Manual landing input prefix for standardize manual mode.
  --manual-output-prefix P     Manual processed output prefix for standardize manual mode.
  --manual-object-name NAME    Optional manual object name override.
  --landing-base-prefix PATH   Override LANDING_BASE_PREFIX for the run.
  --landing-partitions JSON    Override LANDING_PARTITION_FIELDS_JSON for the run.
  --landing-path-suffix JSON   Override LANDING_PATH_SUFFIX_JSON for the run.
  --landing-input-prefix PATH  Override LANDING_INPUT_PREFIX for standardize.
  --processed-base-prefix P    Override PROCESSED_BASE_PREFIX for standardize.
  --processed-partitions JSON  Override PROCESSED_PARTITION_FIELDS_JSON for standardize.
  --processed-path-suffix JSON Override PROCESSED_PATH_SUFFIX_JSON for standardize.
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
STEP_RUNNER="${REPO_ROOT}/scripts/run-ecs-step.sh"

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
SOURCE_ADAPTER_CONFIG_JSON=""
SOURCE_ADAPTER_CONFIG_FILE=""
STANDARDIZE_STRATEGY_CONFIG_JSON=""
STANDARDIZE_STRATEGY_CONFIG_FILE=""
MANUAL_REQUEST_JSON=""
MANUAL_REQUEST_FILE=""
MANUAL_STORAGE_PREFIX=""
MANUAL_INPUT_PREFIX=""
MANUAL_OUTPUT_PREFIX=""
MANUAL_OBJECT_NAME=""
LANDING_BASE_PREFIX=""
LANDING_PARTITIONS_JSON=""
LANDING_PATH_SUFFIX_JSON=""
LANDING_INPUT_PREFIX=""
PROCESSED_BASE_PREFIX=""
PROCESSED_PARTITIONS_JSON=""
PROCESSED_PATH_SUFFIX_JSON=""
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
    --source-adapter-config-json)
      SOURCE_ADAPTER_CONFIG_JSON="${2:-}"
      shift 2
      ;;
    --source-adapter-config-file)
      SOURCE_ADAPTER_CONFIG_FILE="${2:-}"
      shift 2
      ;;
    --standardize-strategy-config-json)
      STANDARDIZE_STRATEGY_CONFIG_JSON="${2:-}"
      shift 2
      ;;
    --standardize-strategy-config-file)
      STANDARDIZE_STRATEGY_CONFIG_FILE="${2:-}"
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
    --manual-input-prefix)
      MANUAL_INPUT_PREFIX="${2:-}"
      shift 2
      ;;
    --manual-output-prefix)
      MANUAL_OUTPUT_PREFIX="${2:-}"
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
    --processed-base-prefix)
      PROCESSED_BASE_PREFIX="${2:-}"
      shift 2
      ;;
    --processed-partitions)
      PROCESSED_PARTITIONS_JSON="${2:-}"
      shift 2
      ;;
    --processed-path-suffix)
      PROCESSED_PATH_SUFFIX_JSON="${2:-}"
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

if [[ -n "$SOURCE_ADAPTER_CONFIG_JSON" && -n "$SOURCE_ADAPTER_CONFIG_FILE" ]]; then
  echo "Use only one of --source-adapter-config-json or --source-adapter-config-file" >&2
  exit 1
fi

if [[ -n "$STANDARDIZE_STRATEGY_CONFIG_JSON" && -n "$STANDARDIZE_STRATEGY_CONFIG_FILE" ]]; then
  echo "Use only one of --standardize-strategy-config-json or --standardize-strategy-config-file" >&2
  exit 1
fi

if [[ -n "$MANUAL_REQUEST_JSON" && -n "$MANUAL_REQUEST_FILE" ]]; then
  echo "Use only one of --manual-request-json or --manual-request-file" >&2
  exit 1
fi

if [[ -n "$SOURCE_ADAPTER_CONFIG_FILE" ]]; then
  SOURCE_ADAPTER_CONFIG_JSON="$(cat "$SOURCE_ADAPTER_CONFIG_FILE")"
fi

if [[ -n "$STANDARDIZE_STRATEGY_CONFIG_FILE" ]]; then
  STANDARDIZE_STRATEGY_CONFIG_JSON="$(cat "$STANDARDIZE_STRATEGY_CONFIG_FILE")"
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

if [[ "$PLANNING_MODE" == "manual" && "$STEP" == "both" ]]; then
  echo "--planning-mode manual requires --step source-ingest or --step standardize" >&2
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

if [[ ! -f "$STEP_RUNNER" ]]; then
  echo "Missing step runner script: ${STEP_RUNNER}" >&2
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
    SOURCE_ADAPTER_CONFIG_JSON="$SOURCE_ADAPTER_CONFIG_JSON" \
    STANDARDIZE_STRATEGY_CONFIG_JSON="$STANDARDIZE_STRATEGY_CONFIG_JSON" \
    MANUAL_REQUEST_JSON="$MANUAL_REQUEST_JSON" \
    MANUAL_STORAGE_PREFIX="$MANUAL_STORAGE_PREFIX" \
    MANUAL_INPUT_PREFIX="$MANUAL_INPUT_PREFIX" \
    MANUAL_OUTPUT_PREFIX="$MANUAL_OUTPUT_PREFIX" \
    MANUAL_OBJECT_NAME="$MANUAL_OBJECT_NAME" \
    LANDING_BASE_PREFIX="$LANDING_BASE_PREFIX" \
    LANDING_PARTITIONS_JSON="$LANDING_PARTITIONS_JSON" \
    LANDING_PATH_SUFFIX_JSON="$LANDING_PATH_SUFFIX_JSON" \
    LANDING_INPUT_PREFIX="$LANDING_INPUT_PREFIX" \
    PROCESSED_BASE_PREFIX="$PROCESSED_BASE_PREFIX" \
    PROCESSED_PARTITIONS_JSON="$PROCESSED_PARTITIONS_JSON" \
    PROCESSED_PATH_SUFFIX_JSON="$PROCESSED_PATH_SUFFIX_JSON" \
    WORKFLOW_NAME="$WORKFLOW_NAME" \
    python3 - <<'PY'
import json
import os
import sys
from datetime import datetime, timezone
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

env.append({"name": "PLANNING_MODE", "value": planning_mode})

if planning_mode == "manual":
    if step_name == "source-ingest":
        manual_request_json = os.environ.get("MANUAL_REQUEST_JSON")
        if manual_request_json:
            env.append({"name": "MANUAL_REQUEST_JSON", "value": manual_request_json})
        for key in ("MANUAL_STORAGE_PREFIX", "MANUAL_OBJECT_NAME"):
            value = os.environ.get(key)
            if value:
                env.append({"name": key, "value": value})
    elif step_name == "standardize":
        for key in ("MANUAL_INPUT_PREFIX", "MANUAL_OUTPUT_PREFIX", "MANUAL_OBJECT_NAME"):
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

source_adapter_config_json = os.environ.get("SOURCE_ADAPTER_CONFIG_JSON")
if source_adapter_config_json and step_name == "source-ingest":
    env.append(
        {
            "name": "SOURCE_ADAPTER_CONFIG_JSON",
            "value": source_adapter_config_json,
        }
    )

standardize_strategy_config_json = os.environ.get("STANDARDIZE_STRATEGY_CONFIG_JSON")
if standardize_strategy_config_json and step_name == "standardize":
    env.append(
        {
            "name": "STANDARDIZE_STRATEGY_CONFIG_JSON",
            "value": standardize_strategy_config_json,
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

    processed_base_prefix = os.environ.get("PROCESSED_BASE_PREFIX")
    if processed_base_prefix:
        env.append(
            {
                "name": "PROCESSED_BASE_PREFIX",
                "value": processed_base_prefix,
            }
        )

    processed_partitions_json = os.environ.get("PROCESSED_PARTITIONS_JSON")
    if processed_partitions_json:
        env.append(
            {
                "name": "PROCESSED_PARTITION_FIELDS_JSON",
                "value": processed_partitions_json,
            }
        )

    processed_path_suffix_json = os.environ.get("PROCESSED_PATH_SUFFIX_JSON")
    if processed_path_suffix_json:
        env.append(
            {
                "name": "PROCESSED_PATH_SUFFIX_JSON",
                "value": processed_path_suffix_json,
            }
        )

started_by = f"manual-{os.environ['WORKFLOW_NAME']}-{step_name}"
created_on = datetime.now(timezone.utc).date().isoformat()

sys.stdout.write(
    json.dumps(
        {
            "region": aws_region,
            "cluster": cluster_arn,
            "task_definition": task_definition,
            "container_name": container_name,
            "subnets": subnets,
            "security_group": security_group,
            "started_by": started_by,
            "tags": [
                "auto_cleanup=true",
                "cleanup_schedule=daily",
                f"created_on={created_on}",
            ],
            "environment": [f"{item['name']}={item['value']}" for item in env],
        }
    )
)
PY
  )"

  local aws_region cluster_arn task_arn
  aws_region="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["region"])' <<<"$cli_input")"
  cluster_arn="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["cluster"])' <<<"$cli_input")"
  mapfile -t step_runner_args < <(
    CLI_INPUT="$cli_input" python3 - <<'PY'
import json
import os
import shlex
import sys

payload = json.loads(os.environ["CLI_INPUT"])
args = [
    "--region", payload["region"],
    "--cluster", payload["cluster"],
    "--task-definition", payload["task_definition"],
    "--container", payload["container_name"],
    "--subnets", json.dumps(payload["subnets"]),
    "--security-group", payload["security_group"],
    "--started-by", payload["started_by"],
]
for tag in payload["tags"]:
    args.extend(["--tag", tag])
for env_item in payload["environment"]:
    args.extend(["--env", env_item])

for arg in args:
    print(arg)
PY
  )

  if [[ "$step_name" != "source-ingest" || "$STEP" != "both" ]] && [[ "$WAIT_FOR_FINAL" == "true" ]]; then
    step_runner_args+=("--wait")
  fi

  local step_output
  step_output="$("$STEP_RUNNER" "${step_runner_args[@]}")"
  task_arn="$(awk '/^  task:/{print $2}' <<<"$step_output")"

  echo "Started ${step_name} task:"
  echo "  workflow: ${WORKFLOW_NAME}"
  echo "  env:      ${ENVIRONMENT}"
  echo "$step_output" | sed 's/^/  /'

  if [[ "$step_name" == "source-ingest" && "$STEP" == "both" ]]; then
    echo "Waiting for source-ingest to finish before starting standardize..."
    aws ecs wait tasks-stopped --region "$aws_region" --cluster "$cluster_arn" --tasks "$task_arn"
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
