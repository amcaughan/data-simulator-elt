#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="$1"
REPOSITORY_URL="$2"
IMAGE_TAG="$3"
DOCKERFILE_PATH="$4"
BUILD_CONTEXT_DIR="$5"

REGISTRY_HOST="${REPOSITORY_URL%%/*}"
IMAGE_URI="${REPOSITORY_URL}:${IMAGE_TAG}"

aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY_HOST"

docker build \
  --file "$DOCKERFILE_PATH" \
  --tag "$IMAGE_URI" \
  "$BUILD_CONTEXT_DIR"

docker push "$IMAGE_URI"
