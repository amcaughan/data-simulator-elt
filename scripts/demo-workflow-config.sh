#!/usr/bin/env bash

set_demo_defaults() {
  local workflow_name="$1"

  DEMO_SLICE_RANGE_START_AT=""
  DEMO_SLICE_RANGE_END_AT=""
  DEMO_QUERY_TABLE=""

  case "$workflow_name" in
    sample-api-polling-01)
      DEMO_SLICE_RANGE_START_AT="2026-03-01T00:00:00Z"
      DEMO_SLICE_RANGE_END_AT="2026-03-02T23:59:59Z"
      DEMO_QUERY_TABLE="management_daily_summary"
      ;;
    sample-file-delivery-01)
      DEMO_SLICE_RANGE_START_AT="2026-03-01T00:00:00Z"
      DEMO_SLICE_RANGE_END_AT="2026-03-02T23:59:59Z"
      DEMO_QUERY_TABLE="management_daily_summary"
      ;;
    *)
      echo "No demo defaults are defined for workflow: ${workflow_name}" >&2
      return 1
      ;;
  esac
}
