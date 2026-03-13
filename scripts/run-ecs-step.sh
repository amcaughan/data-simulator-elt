#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./scripts/run-ecs-step.sh [options]

Run a single ECS task directly when you already know the cluster, task
definition, networking, and container override values.

Required:
  --region NAME                 AWS region for the ECS task
  --cluster ARN                 ECS cluster ARN
  --task-definition ARN         ECS task definition ARN
  --container NAME              Container name to override
  --subnets JSON                JSON array of subnet ids
  --security-group ID           Security group id

Options:
  --env NAME=VALUE              Container environment override. Repeatable.
  --started-by VALUE            ECS startedBy value.
  --tag KEY=VALUE               ECS task tag. Repeatable.
  --wait                        Wait for the task to stop before exiting.
  --help                        Show this message.
EOF
}

AWS_REGION=""
CLUSTER_ARN=""
TASK_DEFINITION_ARN=""
CONTAINER_NAME=""
SUBNETS_JSON=""
SECURITY_GROUP_ID=""
STARTED_BY=""
WAIT_FOR_STOP="false"

declare -a ENV_OVERRIDES=()
declare -a TASK_TAGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --region)
      AWS_REGION="${2:-}"
      shift 2
      ;;
    --cluster)
      CLUSTER_ARN="${2:-}"
      shift 2
      ;;
    --task-definition)
      TASK_DEFINITION_ARN="${2:-}"
      shift 2
      ;;
    --container)
      CONTAINER_NAME="${2:-}"
      shift 2
      ;;
    --subnets)
      SUBNETS_JSON="${2:-}"
      shift 2
      ;;
    --security-group)
      SECURITY_GROUP_ID="${2:-}"
      shift 2
      ;;
    --env)
      ENV_OVERRIDES+=("${2:-}")
      shift 2
      ;;
    --started-by)
      STARTED_BY="${2:-}"
      shift 2
      ;;
    --tag)
      TASK_TAGS+=("${2:-}")
      shift 2
      ;;
    --wait)
      WAIT_FOR_STOP="true"
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

for required_value in \
  AWS_REGION \
  CLUSTER_ARN \
  TASK_DEFINITION_ARN \
  CONTAINER_NAME \
  SUBNETS_JSON \
  SECURITY_GROUP_ID
do
  if [[ -z "${!required_value}" ]]; then
    echo "Missing required argument for ${required_value}" >&2
    usage >&2
    exit 1
  fi
done

PAYLOAD_JSON="$(
  AWS_REGION="$AWS_REGION" \
  CLUSTER_ARN="$CLUSTER_ARN" \
  TASK_DEFINITION_ARN="$TASK_DEFINITION_ARN" \
  CONTAINER_NAME="$CONTAINER_NAME" \
  SUBNETS_JSON="$SUBNETS_JSON" \
  SECURITY_GROUP_ID="$SECURITY_GROUP_ID" \
  STARTED_BY="$STARTED_BY" \
  ENV_OVERRIDES_JSON="$(printf '%s\n' "${ENV_OVERRIDES[@]}" | python3 -c 'import json,sys; print(json.dumps([line.rstrip("\n") for line in sys.stdin if line.rstrip("\n")]))')" \
  TASK_TAGS_JSON="$(printf '%s\n' "${TASK_TAGS[@]}" | python3 -c 'import json,sys; print(json.dumps([line.rstrip("\n") for line in sys.stdin if line.rstrip("\n")]))')" \
  python3 - <<'PY'
import json
import os
import sys


def parse_name_value_pairs(items):
    parsed = []
    for item in items:
        if "=" not in item:
            raise SystemExit(f"Expected NAME=VALUE format, got: {item}")
        name, value = item.split("=", 1)
        if not name:
            raise SystemExit(f"Expected NAME=VALUE format, got: {item}")
        parsed.append({"name": name, "value": value})
    return parsed


subnets = json.loads(os.environ["SUBNETS_JSON"])
if not isinstance(subnets, list) or not all(isinstance(item, str) for item in subnets):
    raise SystemExit("--subnets must be a JSON array of subnet ids")

env_overrides = parse_name_value_pairs(json.loads(os.environ["ENV_OVERRIDES_JSON"]))
task_tags = [{"key": pair["name"], "value": pair["value"]} for pair in parse_name_value_pairs(json.loads(os.environ["TASK_TAGS_JSON"]))]

payload = {
    "cluster": os.environ["CLUSTER_ARN"],
    "launchType": "FARGATE",
    "taskDefinition": os.environ["TASK_DEFINITION_ARN"],
    "networkConfiguration": {
        "awsvpcConfiguration": {
            "subnets": subnets,
            "securityGroups": [os.environ["SECURITY_GROUP_ID"]],
            "assignPublicIp": "DISABLED",
        }
    },
    "overrides": {
        "containerOverrides": [
            {
                "name": os.environ["CONTAINER_NAME"],
                "environment": env_overrides,
            }
        ]
    },
}

started_by = os.environ.get("STARTED_BY")
if started_by:
    payload["startedBy"] = started_by

if task_tags:
    payload["tags"] = task_tags

sys.stdout.write(json.dumps(payload))
PY
)"

PAYLOAD_FILE="$(mktemp)"
trap 'rm -f "$PAYLOAD_FILE"' EXIT
printf '%s' "$PAYLOAD_JSON" > "$PAYLOAD_FILE"

TASK_ARN="$(
  aws ecs run-task \
    --region "$AWS_REGION" \
    --cli-input-json "file://${PAYLOAD_FILE}" \
    --query 'tasks[0].taskArn' \
    --output text
)"

if [[ "$TASK_ARN" == "None" || -z "$TASK_ARN" ]]; then
  echo "Failed to start ECS task" >&2
  exit 1
fi

echo "Started task:"
echo "  cluster: ${CLUSTER_ARN}"
echo "  task:    ${TASK_ARN}"

if [[ "$WAIT_FOR_STOP" == "true" ]]; then
  echo "Waiting for task to stop..."
  aws ecs wait tasks-stopped --region "$AWS_REGION" --cluster "$CLUSTER_ARN" --tasks "$TASK_ARN"
fi
