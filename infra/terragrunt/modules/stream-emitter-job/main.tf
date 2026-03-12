data "aws_ssm_parameter" "simulator_api_url" {
  name = var.simulator_api_url_ssm_param_name
}

locals {
  project_slug   = replace(var.project_name, "_", "-")
  family_name    = "${local.project_slug}-${var.environment}-${var.workflow_name}-stream-emitter"
  log_group_name = "/ecs/${local.family_name}"
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
    sid    = "ReadApiUrlParameter"
    effect = "Allow"

    actions = ["ssm:GetParameter"]

    resources = [data.aws_ssm_parameter.simulator_api_url.arn]
  }

  statement {
    sid    = "PublishToStream"
    effect = "Allow"

    actions = [
      "kinesis:DescribeStreamSummary",
      "kinesis:PutRecord",
      "kinesis:PutRecords",
    ]

    resources = [var.stream_arn]
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
      name      = "stream-emitter"
      image     = var.container_image
      essential = true
      command   = var.command
      environment = [
        {
          name  = "WORKFLOW_NAME"
          value = var.workflow_name
        },
        {
          name  = "PRESET_ID"
          value = var.preset_id
        },
        {
          name  = "EMISSION_RATE_PER_MINUTE"
          value = tostring(var.emission_rate_per_minute)
        },
        {
          name  = "STREAM_NAME"
          value = var.stream_name
        },
        {
          name  = "AWS_REGION"
          value = var.aws_region
        },
      ]
      secrets = [
        {
          name      = "SIMULATOR_API_URL"
          valueFrom = data.aws_ssm_parameter.simulator_api_url.arn
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
