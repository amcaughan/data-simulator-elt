#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="data-simulator-elt-dev"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCKERFILE="$REPO_ROOT/local-dev/docker/Dockerfile"
BUILD_CONTEXT="$REPO_ROOT/local-dev/docker"

usage() {
  cat <<EOF
Usage: $0 [command] [options]

Commands:
  build      Build the dev image
  rebuild    Rebuild the dev image (no cache)
  run        Start an interactive shell
  destroy    Remove the dev image
  help       Show this message

Run options:
  --install none|all
            none:   start quickly without Python dependency installs (default)
            all:    install shared and workflow dbt requirements

Examples:
  $0
  $0 run --install all
  $0 --install all
EOF
}

dependency_command() {
  local install_profile="${1:-none}"

  case "$install_profile" in
    none)
      printf '%s\n' 'exec /bin/bash'
      ;;
    all)
      printf '%s\n' 'shopt -s nullglob; for req in /workspace/containers/shared/*/requirements.txt /workspace/containers/workflows/*/dbt/requirements.txt; do if [[ -s "$req" ]]; then python3 -m pip install --user -r "$req"; fi; done; exec /bin/bash'
      ;;
    *)
      echo "Unknown install profile: $install_profile" >&2
      usage >&2
      exit 1
      ;;
  esac
}

build() {
  if docker buildx version >/dev/null 2>&1; then
    docker buildx build --load -f "$DOCKERFILE" -t "$IMAGE_NAME" "$BUILD_CONTEXT"
  else
    echo "docker buildx is not installed; falling back to legacy docker build" >&2
    docker build -f "$DOCKERFILE" -t "$IMAGE_NAME" "$BUILD_CONTEXT"
  fi
}

rebuild() {
  if docker buildx version >/dev/null 2>&1; then
    docker buildx build --load --no-cache -f "$DOCKERFILE" -t "$IMAGE_NAME" "$BUILD_CONTEXT"
  else
    echo "docker buildx is not installed; falling back to legacy docker build" >&2
    docker build --no-cache -f "$DOCKERFILE" -t "$IMAGE_NAME" "$BUILD_CONTEXT"
  fi
}

destroy() {
  docker rmi -f "$IMAGE_NAME"
}

run() {
  local install_profile="none"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --install)
        [[ $# -ge 2 ]] || { echo "--install requires a value" >&2; exit 1; }
        install_profile="$2"
        shift 2
        ;;
      *)
        echo "Unknown run option: $1" >&2
        usage >&2
        exit 1
        ;;
    esac
  done

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
    /bin/bash -lc "$(dependency_command "$install_profile")"
}

if [[ $# -eq 0 ]]; then
  run
  exit 0
fi

if [[ "$1" == --* ]]; then
  cmd="run"
else
  cmd="$1"
  shift || true
fi

case "$cmd" in
  build) build ;;
  rebuild) rebuild ;;
  run) run "$@" ;;
  destroy) destroy ;;
  help|-h|--help) usage ;;
  *) usage; exit 1 ;;
esac
