#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/teardown-workflow.sh --workflow WORKFLOW [options]

Destroy one workflow stack and optionally the shared core stack for the same
environment. Local release manifests are removed by default so the next rollout
starts from a clean slate.

Required:
  --workflow NAME              Workflow stack under infra/terragrunt/live/<env>/

Options:
  --env NAME                   Environment name. Default: dev
  --include-core               Destroy the shared core stack after the workflow.
  --keep-release-manifests     Keep local build/releases manifest files.
  --help                       Show this message.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENVIRONMENT="dev"
WORKFLOW_NAME=""
INCLUDE_CORE="false"
KEEP_RELEASE_MANIFESTS="false"

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
    --include-core)
      INCLUDE_CORE="true"
      shift
      ;;
    --keep-release-manifests)
      KEEP_RELEASE_MANIFESTS="true"
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

WORKFLOW_DIR="${REPO_ROOT}/infra/terragrunt/live/${ENVIRONMENT}/${WORKFLOW_NAME}"
CORE_DIR="${REPO_ROOT}/infra/terragrunt/live/${ENVIRONMENT}/core"
WORKFLOW_MANIFEST_PATH="${REPO_ROOT}/build/releases/${ENVIRONMENT}/${WORKFLOW_NAME}.json"
CORE_MANIFEST_PATH="${REPO_ROOT}/build/releases/${ENVIRONMENT}/core.json"
RELEASE_ENV_DIR="${REPO_ROOT}/build/releases/${ENVIRONMENT}"

if [[ ! -d "$WORKFLOW_DIR" ]]; then
  echo "Workflow stack directory not found: ${WORKFLOW_DIR}" >&2
  exit 1
fi

run_destroy() {
  local stack_dir="$1"
  (
    cd "$stack_dir"
    rm -rf .terragrunt-cache
    terragrunt --non-interactive destroy -auto-approve
  )
}

echo "Teardown workflow: ${WORKFLOW_NAME}"
echo "Environment:      ${ENVIRONMENT}"

echo
echo "Destroying workflow stack..."
run_destroy "$WORKFLOW_DIR"

if [[ "$INCLUDE_CORE" == "true" ]]; then
  echo
  echo "Destroying core stack..."
  run_destroy "$CORE_DIR"
fi

if [[ "$KEEP_RELEASE_MANIFESTS" != "true" ]]; then
  rm -f "$WORKFLOW_MANIFEST_PATH"
  if [[ "$INCLUDE_CORE" == "true" ]]; then
    rm -f "$CORE_MANIFEST_PATH"
  fi
  rmdir "$RELEASE_ENV_DIR" 2>/dev/null || true
fi
