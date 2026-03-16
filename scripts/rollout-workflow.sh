#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/rollout-workflow.sh --workflow WORKFLOW [options]

Roll out one workflow stack end to end: apply shared/core infrastructure,
publish immutable images, re-apply the workflow stack to pick up the released
image URIs, run a simple healthcheck, and optionally execute one sample run.

Required:
  --workflow NAME              Workflow stack under infra/terragrunt/live/<env>/

Options:
  --env NAME                   Environment name. Default: dev
  --sample-run                 Launch one sample workload after rollout.
  --slice-range-start-at ISO   Override the scheduled demo sample start time.
  --slice-range-end-at ISO     Override the scheduled demo sample end time.
  --stream-emitter-runs N      Override the streaming demo emitter run count.
  --skip-core-apply            Skip terragrunt apply for core.
  --skip-core-release          Skip shared image release for core.
  --skip-workflow-apply        Skip terragrunt apply for the workflow.
  --skip-workflow-release      Skip workflow-owned image release.
  --skip-healthcheck           Skip the post-rollout output healthcheck.
  --help                       Show this message.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# shellcheck source=/dev/null
source "${REPO_ROOT}/scripts/demo/workflow-config.sh"

ENVIRONMENT="dev"
WORKFLOW_NAME=""
SAMPLE_RUN="false"
SLICE_RANGE_START_AT=""
SLICE_RANGE_END_AT=""
STREAM_EMITTER_RUNS=""
SKIP_CORE_APPLY="false"
SKIP_CORE_RELEASE="false"
SKIP_WORKFLOW_APPLY="false"
SKIP_WORKFLOW_RELEASE="false"
SKIP_HEALTHCHECK="false"

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
    --sample-run)
      SAMPLE_RUN="true"
      shift
      ;;
    --slice-range-start-at)
      SLICE_RANGE_START_AT="${2:-}"
      shift 2
      ;;
    --slice-range-end-at)
      SLICE_RANGE_END_AT="${2:-}"
      shift 2
      ;;
    --stream-emitter-runs)
      STREAM_EMITTER_RUNS="${2:-}"
      shift 2
      ;;
    --skip-core-apply)
      SKIP_CORE_APPLY="true"
      shift
      ;;
    --skip-core-release)
      SKIP_CORE_RELEASE="true"
      shift
      ;;
    --skip-workflow-apply)
      SKIP_WORKFLOW_APPLY="true"
      shift
      ;;
    --skip-workflow-release)
      SKIP_WORKFLOW_RELEASE="true"
      shift
      ;;
    --skip-healthcheck)
      SKIP_HEALTHCHECK="true"
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

CORE_DIR="${REPO_ROOT}/infra/terragrunt/live/${ENVIRONMENT}/core"
WORKFLOW_DIR="${REPO_ROOT}/infra/terragrunt/live/${ENVIRONMENT}/${WORKFLOW_NAME}"

if [[ ! -d "$WORKFLOW_DIR" ]]; then
  echo "Workflow stack directory not found: ${WORKFLOW_DIR}" >&2
  exit 1
fi

terragrunt_json_output() {
  local stack_dir="$1"
  (
    cd "$stack_dir"
    terragrunt output -json
  )
}

json_value() {
  local key="$1"
  python3 -c '
import json
import sys

payload = json.load(sys.stdin)
key = sys.argv[1]
value = payload[key]["value"]
if isinstance(value, (dict, list)):
    print(json.dumps(value))
elif value is None:
    print("")
else:
    print(value)
' "$key"
}

json_has_key() {
  local key="$1"
  python3 -c '
import json
import sys

payload = json.load(sys.stdin)
print("true" if sys.argv[1] in payload else "false")
' "$key"
}

run_apply() {
  local stack_dir="$1"
  (
    cd "$stack_dir"
    rm -rf .terragrunt-cache
    terragrunt apply --terragrunt-non-interactive -auto-approve
  )
}

require_output_value() {
  local payload="$1"
  local key="$2"
  local description="$3"
  local value

  value="$(printf '%s' "$payload" | json_value "$key")"
  if [[ -z "$value" ]]; then
    echo "Healthcheck failed: missing ${description} (${key})" >&2
    exit 1
  fi
}

echo "Rollout workflow: ${WORKFLOW_NAME}"
echo "Environment:      ${ENVIRONMENT}"

if [[ "$SKIP_CORE_APPLY" != "true" ]]; then
  echo
  echo "Applying core stack..."
  run_apply "$CORE_DIR"
fi

if [[ "$SKIP_CORE_RELEASE" != "true" ]]; then
  echo
  echo "Releasing shared core images..."
  "${REPO_ROOT}/scripts/release/core-images.sh" \
    --env "$ENVIRONMENT"
fi

if [[ "$SKIP_WORKFLOW_APPLY" != "true" ]]; then
  echo
  echo "Applying workflow stack..."
  run_apply "$WORKFLOW_DIR"
fi

if [[ "$SKIP_WORKFLOW_RELEASE" != "true" ]]; then
  echo
  echo "Releasing workflow images..."
  "${REPO_ROOT}/scripts/release/workflow-images.sh" \
    --env "$ENVIRONMENT" \
    --workflow "$WORKFLOW_NAME"

  echo
  echo "Re-applying workflow stack to pick up released image URIs..."
  run_apply "$WORKFLOW_DIR"
fi

CORE_OUTPUTS="$(terragrunt_json_output "$CORE_DIR")"
WORKFLOW_OUTPUTS="$(terragrunt_json_output "$WORKFLOW_DIR")"

LANDING_BUCKET_NAME="$(printf '%s' "$WORKFLOW_OUTPUTS" | json_value landing_bucket_name)"
PROCESSED_BUCKET_NAME="$(printf '%s' "$WORKFLOW_OUTPUTS" | json_value processed_bucket_name)"
MARTS_BUCKET_NAME="$(printf '%s' "$WORKFLOW_OUTPUTS" | json_value marts_bucket_name)"

echo
echo "Discovered outputs:"
echo "  landing:   s3://${LANDING_BUCKET_NAME}"
echo "  processed: s3://${PROCESSED_BUCKET_NAME}"
echo "  marts:     s3://${MARTS_BUCKET_NAME}"

WORKFLOW_KIND="scheduled"
if [[ "$(printf '%s' "$WORKFLOW_OUTPUTS" | json_has_key stream_emitter_task_definition_arn)" == "true" ]]; then
  WORKFLOW_KIND="streaming"
fi

if [[ "$SKIP_HEALTHCHECK" != "true" ]]; then
  echo
  echo "Running rollout healthcheck..."

  require_output_value "$CORE_OUTPUTS" ecs_cluster_arn "ECS cluster ARN"
  require_output_value "$CORE_OUTPUTS" glue_database_name "Glue database name"
  require_output_value "$CORE_OUTPUTS" athena_workgroup_name "Athena workgroup name"
  require_output_value "$WORKFLOW_OUTPUTS" landing_bucket_name "landing bucket"
  require_output_value "$WORKFLOW_OUTPUTS" processed_bucket_name "processed bucket"
  require_output_value "$WORKFLOW_OUTPUTS" marts_bucket_name "marts bucket"

  if [[ "$WORKFLOW_KIND" == "streaming" ]]; then
    require_output_value "$WORKFLOW_OUTPUTS" stream_emitter_task_definition_arn "stream emitter task definition"
    require_output_value "$WORKFLOW_OUTPUTS" dbt_task_definition_arn "dbt task definition"
  else
    require_output_value "$WORKFLOW_OUTPUTS" source_ingest_task_definition_arn "source-ingest task definition"
    require_output_value "$WORKFLOW_OUTPUTS" standardize_task_definition_arn "standardize task definition"
    require_output_value "$WORKFLOW_OUTPUTS" dbt_task_definition_arn "dbt task definition"
  fi

  echo "Healthcheck passed."
fi

if [[ "$SAMPLE_RUN" == "true" ]]; then
  echo
  echo "Running sample workload..."

  DEMO_DEFAULTS_AVAILABLE="true"
  if ! set_demo_defaults "$WORKFLOW_NAME" >/dev/null 2>&1; then
    DEMO_DEFAULTS_AVAILABLE="false"
  fi

  if [[ "$WORKFLOW_KIND" == "streaming" ]]; then
    if [[ -z "$STREAM_EMITTER_RUNS" ]]; then
      if [[ "$DEMO_DEFAULTS_AVAILABLE" != "true" || -z "${DEMO_STREAM_EMITTER_RUNS:-}" ]]; then
        echo "No default stream emitter run count is configured for ${WORKFLOW_NAME}" >&2
        exit 1
      fi
      STREAM_EMITTER_RUNS="$DEMO_STREAM_EMITTER_RUNS"
    fi

    "${REPO_ROOT}/scripts/run/streaming-workflow.sh" \
      --workflow "$WORKFLOW_NAME" \
      --env "$ENVIRONMENT" \
      --step all \
      --emitter-runs "$STREAM_EMITTER_RUNS" \
      --wait
  else
    if [[ -z "$SLICE_RANGE_START_AT" ]]; then
      if [[ "$DEMO_DEFAULTS_AVAILABLE" != "true" || -z "${DEMO_SLICE_RANGE_START_AT:-}" ]]; then
        echo "No default sample start timestamp is configured for ${WORKFLOW_NAME}" >&2
        exit 1
      fi
      SLICE_RANGE_START_AT="$DEMO_SLICE_RANGE_START_AT"
    fi

    if [[ -z "$SLICE_RANGE_END_AT" ]]; then
      if [[ "$DEMO_DEFAULTS_AVAILABLE" != "true" || -z "${DEMO_SLICE_RANGE_END_AT:-}" ]]; then
        echo "No default sample end timestamp is configured for ${WORKFLOW_NAME}" >&2
        exit 1
      fi
      SLICE_RANGE_END_AT="$DEMO_SLICE_RANGE_END_AT"
    fi

    "${REPO_ROOT}/scripts/run/scheduled-workflow.sh" \
      --workflow "$WORKFLOW_NAME" \
      --env "$ENVIRONMENT" \
      --step all \
      --slice-selector-mode range \
      --slice-range-start-at "$SLICE_RANGE_START_AT" \
      --slice-range-end-at "$SLICE_RANGE_END_AT" \
      --wait
  fi
fi
