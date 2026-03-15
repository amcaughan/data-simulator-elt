#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/run-streaming-workflow.sh --workflow WORKFLOW [options]

Run one-off ECS tasks for a streaming workflow stack without enabling its
scheduler resources.

Required:
  --workflow NAME              Workflow stack under infra/terragrunt/live/<env>/

Options:
  --env NAME                   Environment name. Default: dev
  --region NAME                AWS region for ECS task execution. Default: us-east-2
  --step NAME                  stream-emitter | dbt | all. Default: all
  --emitter-runs N             Number of emitter task runs for stream-emitter/all. Default: 3
  --flush-wait-seconds N       Seconds to wait for Firehose flush before dbt. Default: 90
  --dbt-select SELECTOR        Override DBT_SELECT for the dbt task.
  --dbt-exclude SELECTOR       Override DBT_EXCLUDE for the dbt task.
  --dbt-vars-json JSON         Override DBT_VARS_JSON for the dbt task.
  --dbt-vars-file PATH         Read DBT_VARS_JSON override from a file.
  --dbt-full-refresh           Set DBT_FULL_REFRESH=true for the dbt task.
  --wait                       Wait for the final dbt task when step is dbt or all.
  --help                       Show this message.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
STEP_RUNNER="${REPO_ROOT}/scripts/run-ecs-step.sh"

ENVIRONMENT="dev"
AWS_REGION="us-east-2"
WORKFLOW_NAME=""
STEP="all"
EMITTER_RUNS="3"
FLUSH_WAIT_SECONDS="90"
DBT_SELECT=""
DBT_EXCLUDE=""
DBT_VARS_JSON=""
DBT_VARS_FILE=""
DBT_FULL_REFRESH="false"
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
    --region)
      AWS_REGION="${2:-}"
      shift 2
      ;;
    --step)
      STEP="${2:-}"
      shift 2
      ;;
    --emitter-runs)
      EMITTER_RUNS="${2:-}"
      shift 2
      ;;
    --flush-wait-seconds)
      FLUSH_WAIT_SECONDS="${2:-}"
      shift 2
      ;;
    --dbt-select)
      DBT_SELECT="${2:-}"
      shift 2
      ;;
    --dbt-exclude)
      DBT_EXCLUDE="${2:-}"
      shift 2
      ;;
    --dbt-vars-json)
      DBT_VARS_JSON="${2:-}"
      shift 2
      ;;
    --dbt-vars-file)
      DBT_VARS_FILE="${2:-}"
      shift 2
      ;;
    --dbt-full-refresh)
      DBT_FULL_REFRESH="true"
      shift
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
  stream-emitter|dbt|all) ;;
  *)
    echo "--step must be one of: stream-emitter, dbt, all" >&2
    exit 1
    ;;
esac

if [[ -n "$DBT_VARS_JSON" && -n "$DBT_VARS_FILE" ]]; then
  echo "Use only one of --dbt-vars-json or --dbt-vars-file" >&2
  exit 1
fi

if [[ -n "$DBT_VARS_FILE" ]]; then
  DBT_VARS_JSON="$(cat "$DBT_VARS_FILE")"
fi

if ! [[ "$EMITTER_RUNS" =~ ^[0-9]+$ ]] || [[ "$EMITTER_RUNS" -lt 1 ]]; then
  echo "--emitter-runs must be a positive integer" >&2
  exit 1
fi

if ! [[ "$FLUSH_WAIT_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "--flush-wait-seconds must be a non-negative integer" >&2
  exit 1
fi

STACK_DIR="${REPO_ROOT}/infra/terragrunt/live/${ENVIRONMENT}/${WORKFLOW_NAME}"
CORE_DIR="${REPO_ROOT}/infra/terragrunt/live/${ENVIRONMENT}/core"

if [[ ! -d "$STACK_DIR" ]]; then
  echo "Workflow stack not found: ${STACK_DIR}" >&2
  exit 1
fi

WORKFLOW_OUTPUTS_FILE="$(mktemp)"
CORE_OUTPUTS_FILE="$(mktemp)"
trap 'rm -f "$WORKFLOW_OUTPUTS_FILE" "$CORE_OUTPUTS_FILE"' EXIT

(
  cd "$STACK_DIR"
  terragrunt output -json > "$WORKFLOW_OUTPUTS_FILE"
)

(
  cd "$CORE_DIR"
  terragrunt output -json > "$CORE_OUTPUTS_FILE"
)

json_value() {
  local key="$1"
  local file_path="$2"
  python3 -c '
import json
import sys

with open(sys.argv[2], "r", encoding="utf-8") as handle:
    payload = json.load(handle)

value = payload[sys.argv[1]]["value"]
if isinstance(value, (dict, list)):
    print(json.dumps(value))
elif value is None:
    print("")
else:
    print(value)
' "$key" "$file_path"
}

CLUSTER_ARN="$(json_value ecs_cluster_arn "$CORE_OUTPUTS_FILE")"
SUBNETS_JSON="$(json_value network_private_subnet_ids "$CORE_OUTPUTS_FILE")"
SECURITY_GROUP_ID="$(json_value network_security_group_id "$CORE_OUTPUTS_FILE")"
STREAM_EMITTER_TASK_DEFINITION_ARN="$(json_value stream_emitter_task_definition_arn "$WORKFLOW_OUTPUTS_FILE")"
DBT_TASK_DEFINITION_ARN="$(json_value dbt_task_definition_arn "$WORKFLOW_OUTPUTS_FILE")"

run_emitter_once() {
  local run_number="$1"
  "${STEP_RUNNER}" \
    --region "$AWS_REGION" \
    --cluster "$CLUSTER_ARN" \
    --task-definition "$STREAM_EMITTER_TASK_DEFINITION_ARN" \
    --container stream-emitter \
    --subnets "$SUBNETS_JSON" \
    --security-group "$SECURITY_GROUP_ID" \
    --started-by "manual-${WORKFLOW_NAME}-stream-emitter" \
    --tag auto_cleanup=true \
    --tag cleanup_schedule=daily \
    --tag created_on="$(date -u +%Y-%m-%d)" \
    --tag workflow_name="$WORKFLOW_NAME" \
    --tag workflow_step=stream-emitter \
    --tag workflow_run="$run_number" \
    --wait
}

run_dbt_task() {
  local -a args=(
    "${STEP_RUNNER}"
    --region "$AWS_REGION"
    --cluster "$CLUSTER_ARN"
    --task-definition "$DBT_TASK_DEFINITION_ARN"
    --container dbt
    --subnets "$SUBNETS_JSON"
    --security-group "$SECURITY_GROUP_ID"
    --started-by "manual-${WORKFLOW_NAME}-dbt"
    --tag auto_cleanup=true
    --tag cleanup_schedule=daily
    --tag created_on="$(date -u +%Y-%m-%d)"
    --tag workflow_name="$WORKFLOW_NAME"
    --tag workflow_step=dbt
  )

  if [[ -n "$DBT_SELECT" ]]; then
    args+=(--env "DBT_SELECT=${DBT_SELECT}")
  fi
  if [[ -n "$DBT_EXCLUDE" ]]; then
    args+=(--env "DBT_EXCLUDE=${DBT_EXCLUDE}")
  fi
  if [[ "$DBT_FULL_REFRESH" == "true" ]]; then
    args+=(--env "DBT_FULL_REFRESH=true")
  fi
  if [[ -n "$DBT_VARS_JSON" ]]; then
    args+=(--env "DBT_VARS_JSON=${DBT_VARS_JSON}")
  fi
  if [[ "$WAIT_FOR_FINAL" == "true" ]]; then
    args+=(--wait)
  fi

  "${args[@]}"
}

case "$STEP" in
  stream-emitter)
    for ((run_number = 1; run_number <= EMITTER_RUNS; run_number++)); do
      echo "Starting stream-emitter run ${run_number}/${EMITTER_RUNS}..."
      run_emitter_once "$run_number"
    done
    ;;
  dbt)
    echo "Starting dbt task..."
    run_dbt_task
    ;;
  all)
    for ((run_number = 1; run_number <= EMITTER_RUNS; run_number++)); do
      echo "Starting stream-emitter run ${run_number}/${EMITTER_RUNS}..."
      run_emitter_once "$run_number"
    done
    if [[ "$FLUSH_WAIT_SECONDS" -gt 0 ]]; then
      echo "Waiting ${FLUSH_WAIT_SECONDS}s for Firehose to flush stream objects..."
      sleep "$FLUSH_WAIT_SECONDS"
    fi
    echo "Starting dbt task..."
    run_dbt_task
    ;;
esac
