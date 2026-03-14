#!/usr/bin/env bash
set -euo pipefail

DBT_COMMAND="${DBT_COMMAND:-run}"

DBT_ARGS=(
  "${DBT_COMMAND}"
  --project-dir /app/dbt
  --profiles-dir /app/dbt
)

if [[ -n "${DBT_SELECT:-}" ]]; then
  DBT_ARGS+=(--select "${DBT_SELECT}")
fi

if [[ -n "${DBT_EXCLUDE:-}" ]]; then
  DBT_ARGS+=(--exclude "${DBT_EXCLUDE}")
fi

if [[ "${DBT_FULL_REFRESH:-false}" == "true" ]]; then
  DBT_ARGS+=(--full-refresh)
fi

if [[ -n "${DBT_VARS_JSON:-}" ]]; then
  DBT_ARGS+=(--vars "${DBT_VARS_JSON}")
fi

exec dbt "${DBT_ARGS[@]}"
