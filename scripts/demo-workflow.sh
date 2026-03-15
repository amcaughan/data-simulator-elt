#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/demo-workflow.sh --workflow WORKFLOW [options]

Applies the shared core and one workflow stack, runs one sample backfill, and
executes an Athena sanity query against the workflow's default or requested
table.

Required:
  --workflow NAME              Workflow stack under infra/terragrunt/live/<env>/

Options:
  --env NAME                   Environment name. Default: dev
  --query-table NAME           Table name for the final Athena query.
  --query-sql SQL              Full Athena query override.
  --slice-range-start-at ISO   Override the workflow demo start timestamp.
  --slice-range-end-at ISO     Override the workflow demo end timestamp.
  --skip-core-apply            Skip terragrunt apply for core.
  --skip-workflow-apply        Skip terragrunt apply for the workflow.
  --help                       Show this message.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# shellcheck source=/dev/null
source "${SCRIPT_DIR}/demo-workflow-config.sh"

ENVIRONMENT="dev"
WORKFLOW_NAME=""
QUERY_TABLE=""
QUERY_SQL=""
SLICE_RANGE_START_AT=""
SLICE_RANGE_END_AT=""
SKIP_CORE_APPLY="false"
SKIP_WORKFLOW_APPLY="false"

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
    --query-table)
      QUERY_TABLE="${2:-}"
      shift 2
      ;;
    --query-sql)
      QUERY_SQL="${2:-}"
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
    --skip-core-apply)
      SKIP_CORE_APPLY="true"
      shift
      ;;
    --skip-workflow-apply)
      SKIP_WORKFLOW_APPLY="true"
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

set_demo_defaults "$WORKFLOW_NAME"

if [[ -z "$SLICE_RANGE_START_AT" ]]; then
  SLICE_RANGE_START_AT="$DEMO_SLICE_RANGE_START_AT"
fi
if [[ -z "$SLICE_RANGE_END_AT" ]]; then
  SLICE_RANGE_END_AT="$DEMO_SLICE_RANGE_END_AT"
fi
if [[ -z "$QUERY_TABLE" ]]; then
  QUERY_TABLE="$DEMO_QUERY_TABLE"
fi

if [[ -z "$QUERY_SQL" && -z "$QUERY_TABLE" ]]; then
  echo "No default query table is configured for workflow ${WORKFLOW_NAME}" >&2
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

run_apply() {
  local stack_dir="$1"
  (
    cd "$stack_dir"
    rm -rf .terragrunt-cache
    terragrunt apply --terragrunt-non-interactive -auto-approve
  )
}

ATHENA_QUERY_RESULT() {
  local sql="$1"
  local database="$2"
  local workgroup="$3"
  local output_location="$4"
  local query_id

  query_id="$(
    aws athena start-query-execution \
      --work-group "$workgroup" \
      --query-execution-context "Database=${database}" \
      --result-configuration "OutputLocation=${output_location}" \
      --query-string "$sql" \
      --query 'QueryExecutionId' \
      --output text
  )"

  aws athena wait query-succeeded --query-execution-id "$query_id"

  aws athena get-query-results \
    --query-execution-id "$query_id" \
    --query 'ResultSet.Rows[*].Data[*].VarCharValue' \
    --output text
}

echo "Demo workflow: ${WORKFLOW_NAME}"
echo "Environment:   ${ENVIRONMENT}"

if [[ "$SKIP_CORE_APPLY" != "true" ]]; then
  echo
  echo "Applying core stack..."
  run_apply "$CORE_DIR"
fi

if [[ "$SKIP_WORKFLOW_APPLY" != "true" ]]; then
  echo
  echo "Applying workflow stack..."
  run_apply "$WORKFLOW_DIR"
fi

CORE_OUTPUTS="$(terragrunt_json_output "$CORE_DIR")"
WORKFLOW_OUTPUTS="$(terragrunt_json_output "$WORKFLOW_DIR")"

GLUE_DATABASE_NAME="$(printf '%s' "$CORE_OUTPUTS" | json_value glue_database_name)"
ATHENA_WORKGROUP_NAME="$(printf '%s' "$CORE_OUTPUTS" | json_value athena_workgroup_name)"
ATHENA_RESULTS_BUCKET_NAME="$(printf '%s' "$CORE_OUTPUTS" | json_value athena_results_bucket_name)"
LANDING_BUCKET_NAME="$(printf '%s' "$WORKFLOW_OUTPUTS" | json_value landing_bucket_name)"
PROCESSED_BUCKET_NAME="$(printf '%s' "$WORKFLOW_OUTPUTS" | json_value processed_bucket_name)"
MARTS_BUCKET_NAME="$(printf '%s' "$WORKFLOW_OUTPUTS" | json_value marts_bucket_name)"

echo
echo "Discovered outputs:"
echo "  landing:   s3://${LANDING_BUCKET_NAME}"
echo "  processed: s3://${PROCESSED_BUCKET_NAME}"
echo "  marts:     s3://${MARTS_BUCKET_NAME}"
echo "  database:  ${GLUE_DATABASE_NAME}"
echo "  workgroup: ${ATHENA_WORKGROUP_NAME}"

echo
echo "Running sample workflow slice window..."
"${REPO_ROOT}/scripts/run-scheduled-workflow.sh" \
  --workflow "$WORKFLOW_NAME" \
  --env "$ENVIRONMENT" \
  --step all \
  --slice-selector-mode range \
  --slice-range-start-at "$SLICE_RANGE_START_AT" \
  --slice-range-end-at "$SLICE_RANGE_END_AT" \
  --wait

if [[ -n "$QUERY_SQL" ]]; then
  FINAL_QUERY_SQL="$QUERY_SQL"
else
  FINAL_QUERY_SQL="select count(*) as row_count from ${QUERY_TABLE}"
fi

echo
echo "Athena query:"
echo "  ${FINAL_QUERY_SQL}"
echo

ATHENA_QUERY_RESULT \
  "$FINAL_QUERY_SQL" \
  "$GLUE_DATABASE_NAME" \
  "$ATHENA_WORKGROUP_NAME" \
  "s3://${ATHENA_RESULTS_BUCKET_NAME}/query-results/"
