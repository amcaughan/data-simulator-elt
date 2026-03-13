data "aws_ssm_parameter" "source_base_url" {
  count = var.source_base_url_ssm_param_name == null ? 0 : 1
  name  = var.source_base_url_ssm_param_name
}

data "aws_caller_identity" "current" {}

locals {
  project_slug   = replace(var.project_name, "_", "-")
  family_name    = "${local.project_slug}-${var.environment}-${var.workflow_name}-source-ingest"
  log_group_name = "/ecs/${local.family_name}"
  source_base_url_value = try(data.aws_ssm_parameter.source_base_url[0].value, null)
  execute_api_parts = (
    var.source_adapter == "simulator_api" && local.source_base_url_value != null
    ? regex("https://([^.]+)\\.execute-api\\.[^.]+\\.amazonaws\\.com/([^/]+)$", local.source_base_url_value)
    : []
  )
  simulator_api_invoke_arn = length(local.execute_api_parts) == 2 ? "arn:aws:execute-api:${var.aws_region}:${data.aws_caller_identity.current.account_id}:${local.execute_api_parts[0]}/${local.execute_api_parts[1]}/POST/v1/presets/*/generate" : null
  environment = concat(
    [
      {
        name  = "WORKFLOW_NAME"
        value = var.workflow_name
      },
      {
        name  = "SOURCE_ADAPTER"
        value = var.source_adapter
      },
      {
        name  = "SLICE_GRANULARITY"
        value = var.slice_granularity
      },
      {
        name  = "MODE"
        value = var.mode
      },
      {
        name  = "LANDING_BUCKET_NAME"
        value = var.landing_bucket_name
      },
      {
        name  = "AWS_REGION"
        value = var.aws_region
      },
      {
        name  = "SOURCE_ADAPTER_CONFIG_JSON"
        value = var.source_adapter_config_json
      },
    ],
    var.landing_base_prefix == null ? [] : [
      {
        name  = "LANDING_BASE_PREFIX"
        value = var.landing_base_prefix
      }
    ],
    var.landing_partition_fields_json == null ? [] : [
      {
        name  = "LANDING_PARTITION_FIELDS_JSON"
        value = var.landing_partition_fields_json
      }
    ],
    var.landing_path_suffix_json == null ? [] : [
      {
        name  = "LANDING_PATH_SUFFIX_JSON"
        value = var.landing_path_suffix_json
      }
    ],
    var.logical_date == null ? [] : [
      {
        name  = "LOGICAL_DATE"
        value = var.logical_date
      }
    ],
    var.start_at == null ? [] : [
      {
        name  = "START_AT"
        value = var.start_at
      }
    ],
    var.end_at == null ? [] : [
      {
        name  = "END_AT"
        value = var.end_at
      }
    ],
    var.backfill_count == null ? [] : [
      {
        name  = "BACKFILL_COUNT"
        value = tostring(var.backfill_count)
      }
    ],
  )
}

data "aws_iam_policy_document" "task_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_cloudwatch_log_group" "this" {
  name              = local.log_group_name
  retention_in_days = var.log_retention_in_days
}

resource "aws_iam_role" "execution" {
  name               = "${local.family_name}-execution"
  assume_role_policy = data.aws_iam_policy_document.task_assume_role.json
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "task_policy" {
  statement {
    sid    = "LandingBucketAccess"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:ListBucket",
      "s3:PutObject",
    ]

    resources = [
      "arn:aws:s3:::${var.landing_bucket_name}",
      "arn:aws:s3:::${var.landing_bucket_name}/*",
    ]
  }

  dynamic "statement" {
    for_each = var.source_base_url_ssm_param_name == null ? [] : [1]

    content {
      sid    = "ReadSourceBaseUrlParameter"
      effect = "Allow"

      actions = ["ssm:GetParameter"]

      resources = [data.aws_ssm_parameter.source_base_url[0].arn]
    }
  }

  dynamic "statement" {
    for_each = local.simulator_api_invoke_arn == null ? [] : [1]

    content {
      sid    = "InvokeSimulatorApi"
      effect = "Allow"

      actions = ["execute-api:Invoke"]

      resources = [local.simulator_api_invoke_arn]
    }
  }
}

resource "aws_iam_role" "task" {
  name               = "${local.family_name}-task"
  assume_role_policy = data.aws_iam_policy_document.task_assume_role.json
}

resource "aws_iam_role_policy" "task" {
  name   = "${local.family_name}-task"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task_policy.json
}

resource "aws_ecs_task_definition" "this" {
  family                   = local.family_name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.cpu)
  memory                   = tostring(var.memory)
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([
    {
      name      = "source-ingest"
      image     = var.container_image
      essential = true
      command   = var.command
      environment = local.environment
      secrets = [
        for parameter in data.aws_ssm_parameter.source_base_url : {
          name      = "SOURCE_BASE_URL"
          valueFrom = parameter.arn
        }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.this.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])
}
