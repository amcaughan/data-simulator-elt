#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/release-workflow-images.sh --workflow WORKFLOW [--env ENV] [--region AWS_REGION]

Builds and pushes workflow-owned images, then writes their immutable URIs into
build/releases/<env>/<workflow>.json for later Terragrunt applies.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/release-lib.sh"

ENVIRONMENT="dev"
AWS_REGION="${AWS_REGION:-}"
WORKFLOW_NAME=""

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

require_command aws
require_command docker
require_command terragrunt

if [[ -z "$AWS_REGION" ]]; then
  AWS_REGION="$(aws configure get region 2>/dev/null || true)"
fi

if [[ -z "$AWS_REGION" ]]; then
  AWS_REGION="us-east-2"
fi

WORKFLOW_STACK_DIR="${REPO_ROOT}/infra/terragrunt/live/${ENVIRONMENT}/${WORKFLOW_NAME}"
WORKFLOW_ROOT_DIR="${REPO_ROOT}/containers/workflows/${WORKFLOW_NAME}"
MANIFEST_PATH="${REPO_ROOT}/build/releases/${ENVIRONMENT}/${WORKFLOW_NAME}.json"

if [[ ! -d "$WORKFLOW_STACK_DIR" ]]; then
  echo "Workflow stack not found: ${WORKFLOW_STACK_DIR}" >&2
  exit 1
fi

if [[ ! -d "$WORKFLOW_ROOT_DIR" ]]; then
  echo "Workflow source directory not found: ${WORKFLOW_ROOT_DIR}" >&2
  exit 1
fi

DBT_DOCKERFILE="${WORKFLOW_ROOT_DIR}/dbt/Dockerfile"
STREAM_EMITTER_DOCKERFILE="${WORKFLOW_ROOT_DIR}/stream_emitter/Dockerfile"

DBT_IMAGE_URI=""
STREAM_EMITTER_IMAGE_URI=""

if [[ -f "$DBT_DOCKERFILE" ]]; then
  DBT_REPOSITORY_URL="$(terragrunt_output_raw "$WORKFLOW_STACK_DIR" dbt_ecr_repository_url)"
  DBT_IMAGE_URI="$(
    release_image \
      "$AWS_REGION" \
      "$DBT_REPOSITORY_URL" \
      "$DBT_DOCKERFILE" \
      "${WORKFLOW_ROOT_DIR}/dbt" \
      "${WORKFLOW_ROOT_DIR}/dbt"
  )"
fi

if [[ -f "$STREAM_EMITTER_DOCKERFILE" ]]; then
  STREAM_EMITTER_REPOSITORY_URL="$(terragrunt_output_raw "$WORKFLOW_STACK_DIR" stream_emitter_ecr_repository_url)"
  STREAM_EMITTER_IMAGE_URI="$(
    release_image \
      "$AWS_REGION" \
      "$STREAM_EMITTER_REPOSITORY_URL" \
      "$STREAM_EMITTER_DOCKERFILE" \
      "${WORKFLOW_ROOT_DIR}/stream_emitter" \
      "${WORKFLOW_ROOT_DIR}/stream_emitter"
  )"
fi

if [[ -z "$DBT_IMAGE_URI" && -z "$STREAM_EMITTER_IMAGE_URI" ]]; then
  echo "No releasable workflow images found for ${WORKFLOW_NAME}" >&2
  exit 1
fi

if [[ -n "$DBT_IMAGE_URI" && -n "$STREAM_EMITTER_IMAGE_URI" ]]; then
  write_release_manifest \
    "$MANIFEST_PATH" \
    dbt_image_uri "$DBT_IMAGE_URI" \
    stream_emitter_image_uri "$STREAM_EMITTER_IMAGE_URI"
elif [[ -n "$DBT_IMAGE_URI" ]]; then
  write_release_manifest \
    "$MANIFEST_PATH" \
    dbt_image_uri "$DBT_IMAGE_URI"
else
  write_release_manifest \
    "$MANIFEST_PATH" \
    stream_emitter_image_uri "$STREAM_EMITTER_IMAGE_URI"
fi

echo "Wrote ${MANIFEST_PATH}"
