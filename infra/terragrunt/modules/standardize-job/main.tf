locals {
  project_slug   = replace(var.project_name, "_", "-")
  family_name    = "${local.project_slug}-${var.environment}-${var.workflow_name}-standardize"
  log_group_name = "/ecs/${local.family_name}"
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
        name  = "SOURCE_ADAPTER_CONFIG_JSON"
        value = var.source_adapter_config_json
      },
      {
        name  = "LANDING_BUCKET_NAME"
        value = var.landing_bucket_name
      },
      {
        name  = "PROCESSED_BUCKET_NAME"
        value = var.processed_bucket_name
      },
      {
        name  = "AWS_REGION"
        value = var.aws_region
      },
      {
        name  = "LANDING_PARTITION_GRANULARITY"
        value = var.landing_partition_granularity
      },
      {
        name  = "OUTPUT_PARTITION_GRANULARITY"
        value = var.output_partition_granularity
      },
      {
        name  = "MODE"
        value = var.mode
      },
      {
        name  = "PROCESSED_OUTPUT_PREFIX"
        value = var.processed_output_prefix
      },
    ],
    var.landing_input_prefix == null ? [] : [
      {
        name  = "LANDING_INPUT_PREFIX"
        value = var.landing_input_prefix
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
    var.backfill_days == null ? [] : [
      {
        name  = "BACKFILL_DAYS"
        value = tostring(var.backfill_days)
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
    sid    = "ReadLandingObjects"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]

    resources = [
      "arn:aws:s3:::${var.landing_bucket_name}",
      "arn:aws:s3:::${var.landing_bucket_name}/*",
    ]
  }

  statement {
    sid    = "WriteProcessedObjects"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:ListBucket",
      "s3:PutObject",
      "s3:DeleteObject",
    ]

    resources = [
      "arn:aws:s3:::${var.processed_bucket_name}",
      "arn:aws:s3:::${var.processed_bucket_name}/*",
    ]
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
      name        = "standardize"
      image       = var.container_image
      essential   = true
      command     = var.command
      environment = local.environment
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
