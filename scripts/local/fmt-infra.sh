#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/local/fmt-infra.sh [--check]

Format or check Terraform and Terragrunt files for this repo.

Options:
  --check   Run in CI-style check mode instead of rewriting files
EOF
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
terragrunt_dir="${repo_root}/infra/terragrunt"
terraform_modules_dir="${terragrunt_dir}/modules"

check_mode=false

while (($# > 0)); do
  case "$1" in
    --check)
      check_mode=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -d "${terraform_modules_dir}" ]]; then
  echo "Terraform modules directory not found: ${terraform_modules_dir}" >&2
  exit 1
fi

if [[ ! -d "${terragrunt_dir}" ]]; then
  echo "Terragrunt directory not found: ${terragrunt_dir}" >&2
  exit 1
fi

if [[ "${check_mode}" == true ]]; then
  terraform fmt -check -recursive "${terraform_modules_dir}"
  (
    cd "${terragrunt_dir}"
    terragrunt hcl format --check
  )
else
  terraform fmt -recursive "${terraform_modules_dir}"
  (
    cd "${terragrunt_dir}"
    terragrunt hcl format
  )
fi
