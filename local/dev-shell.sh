#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="data-simulator-elt-dev"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCKERFILE="$REPO_ROOT/local/Dockerfile"
BUILD_CONTEXT="$REPO_ROOT/local"

usage() {
  cat <<EOF
Usage: $0 <command>

Commands:
  build      Build the dev image
  rebuild    Rebuild the dev image (no cache)
  run        Start an interactive shell
  destroy    Remove the dev image
  help       Show this message
EOF
}

build() {
  docker build -f "$DOCKERFILE" -t "$IMAGE_NAME" "$BUILD_CONTEXT"
}

rebuild() {
  docker build --no-cache -f "$DOCKERFILE" -t "$IMAGE_NAME" "$BUILD_CONTEXT"
}

destroy() {
  docker rmi -f "$IMAGE_NAME"
}

run() {
  if ! docker image inspect "$IMAGE_NAME" >/dev/null 2>&1; then
    build
  fi

  local docker_args=(
    --rm
    -it
    -v "$REPO_ROOT:/workspace"
    -w /workspace
    -v "$HOME/.aws:/home/dev/.aws:rw"
  )

  if [[ -S /var/run/docker.sock ]]; then
    docker_args+=(
      -v /var/run/docker.sock:/var/run/docker.sock
      --group-add "$(stat -c '%g' /var/run/docker.sock)"
    )
  fi

  docker run "${docker_args[@]}" \
    "$IMAGE_NAME" \
    /bin/bash -lc 'shopt -s nullglob; for req in /workspace/jobs/*/requirements.txt /workspace/workflows/*/dbt/requirements.txt; do if [[ -s "$req" ]]; then python3 -m pip install --user -r "$req"; fi; done; exec /bin/bash'
}

if [[ $# -eq 0 ]]; then
  run
  exit 0
fi

cmd="$1"
shift || true

case "$cmd" in
  build) build ;;
  rebuild) rebuild ;;
  run) run ;;
  destroy) destroy ;;
  help|-h|--help) usage ;;
  *) usage; exit 1 ;;
esac
