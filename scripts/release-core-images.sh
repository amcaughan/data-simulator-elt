#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/release-core-images.sh [--env ENV] [--region AWS_REGION]

Builds and pushes the shared source-ingest and standardize images, then writes
their immutable URIs into build/releases/<env>/core.json for later Terragrunt
applies.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SCRIPT_DIR}/release-lib.sh"

ENVIRONMENT="dev"
AWS_REGION="${AWS_REGION:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
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

require_command aws
require_command docker
require_command terragrunt

if [[ -z "$AWS_REGION" ]]; then
  AWS_REGION="$(aws configure get region 2>/dev/null || true)"
fi

if [[ -z "$AWS_REGION" ]]; then
  AWS_REGION="us-east-2"
fi

CORE_STACK_DIR="${REPO_ROOT}/infra/terragrunt/live/${ENVIRONMENT}/core"
MANIFEST_PATH="${REPO_ROOT}/build/releases/${ENVIRONMENT}/core.json"

if [[ ! -d "$CORE_STACK_DIR" ]]; then
  echo "Core stack not found: ${CORE_STACK_DIR}" >&2
  exit 1
fi

SOURCE_INGEST_REPOSITORY_URL="$(terragrunt_output_raw "$CORE_STACK_DIR" source_ingest_ecr_repository_url)"
STANDARDIZE_REPOSITORY_URL="$(terragrunt_output_raw "$CORE_STACK_DIR" standardize_ecr_repository_url)"

SOURCE_INGEST_IMAGE_URI="$(
  release_image \
    "$AWS_REGION" \
    "$SOURCE_INGEST_REPOSITORY_URL" \
    "${REPO_ROOT}/containers/shared/source_ingest/Dockerfile" \
    "${REPO_ROOT}/containers/shared" \
    "${REPO_ROOT}/containers/shared/source_ingest" \
    "${REPO_ROOT}/containers/shared/common"
)"

STANDARDIZE_IMAGE_URI="$(
  release_image \
    "$AWS_REGION" \
    "$STANDARDIZE_REPOSITORY_URL" \
    "${REPO_ROOT}/containers/shared/standardize/Dockerfile" \
    "${REPO_ROOT}/containers/shared" \
    "${REPO_ROOT}/containers/shared/standardize" \
    "${REPO_ROOT}/containers/shared/common"
)"

write_release_manifest \
  "$MANIFEST_PATH" \
  source_ingest_image_uri "$SOURCE_INGEST_IMAGE_URI" \
  standardize_image_uri "$STANDARDIZE_IMAGE_URI"

echo "Wrote ${MANIFEST_PATH}"
