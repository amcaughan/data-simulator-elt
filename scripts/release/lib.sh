#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

require_command() {
  local command_name="$1"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Required command not found: ${command_name}" >&2
    exit 1
  fi
}

terragrunt_output_raw() {
  local stack_dir="$1"
  local output_name="$2"

  (
    cd "$stack_dir"
    terragrunt --non-interactive output -raw "$output_name" | awk 'NF { line = $0 } END { print line }'
  )
}

compute_source_hash() {
  REPO_ROOT="$REPO_ROOT" python3 - "$@" <<'PY'
import hashlib
import os
import sys
from pathlib import Path

repo_root = Path(os.environ["REPO_ROOT"]).resolve()
hash_paths = [Path(path).resolve() for path in sys.argv[1:]]

entries = []
for base_path in hash_paths:
    if base_path.is_file():
        relative_path = base_path.relative_to(repo_root).as_posix()
        digest = hashlib.sha256(base_path.read_bytes()).hexdigest()
        entries.append(f"{relative_path}:{digest}")
        continue

    for file_path in sorted(path for path in base_path.rglob("*") if path.is_file()):
        relative_path = file_path.relative_to(repo_root).as_posix()
        digest = hashlib.sha256(file_path.read_bytes()).hexdigest()
        entries.append(f"{relative_path}:{digest}")

final_hash = hashlib.sha256(",".join(entries).encode("utf-8")).hexdigest()
print(final_hash)
PY
}

ecr_image_tag_exists() {
  local aws_region="$1"
  local repository_name="$2"
  local image_tag="$3"

  aws ecr describe-images \
    --region "$aws_region" \
    --repository-name "$repository_name" \
    --image-ids "imageTag=${image_tag}" \
    >/dev/null 2>&1
}

ensure_ecr_login() {
  local aws_region="$1"
  local repository_url="$2"
  local registry_host="${repository_url%%/*}"

  aws ecr get-login-password --region "$aws_region" \
    | docker login --username AWS --password-stdin "$registry_host" >/dev/null
}

release_image() {
  local aws_region="$1"
  local repository_url="$2"
  local dockerfile_path="$3"
  local build_context_dir="$4"
  shift 4
  local -a hash_paths=("$@")

  local source_hash
  local image_tag
  local image_uri
  local repository_name

  source_hash="$(compute_source_hash "${hash_paths[@]}")"
  image_tag="sha-${source_hash:0:12}"
  image_uri="${repository_url}:${image_tag}"
  repository_name="${repository_url##*/}"

  if ecr_image_tag_exists "$aws_region" "$repository_name" "$image_tag"; then
    echo "Reusing existing image ${image_uri}" >&2
    printf '%s\n' "$image_uri"
    return 0
  fi

  ensure_ecr_login "$aws_region" "$repository_url"

  docker build \
    --file "$dockerfile_path" \
    --tag "$image_uri" \
    "$build_context_dir" >/dev/null

  docker push "$image_uri" >/dev/null

  echo "Published image ${image_uri}" >&2
  printf '%s\n' "$image_uri"
}

write_release_manifest() {
  local manifest_path="$1"
  shift

  mkdir -p "$(dirname "$manifest_path")"

  {
    echo "{"
    local first_entry="true"
    while [[ $# -gt 0 ]]; do
      local key="$1"
      local value="$2"
      shift 2

      if [[ "$first_entry" == "true" ]]; then
        first_entry="false"
      else
        echo ","
      fi

      printf '  "%s": "%s"' "$key" "$value"
    done
    echo
    echo "}"
  } > "$manifest_path"
}
