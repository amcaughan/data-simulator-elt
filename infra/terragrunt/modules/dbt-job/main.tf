locals {
  project_slug         = replace(var.project_name, "_", "-")
  workflow_token       = "${substr(replace(var.workflow_name, "-", ""), 0, 3)}${substr(md5(var.workflow_name), 0, 5)}"
  family_name          = "${local.project_slug}-${var.environment}-${local.workflow_token}-dbt"
  log_group_name       = "/ecs/${local.family_name}"
  athena_workgroup_arn = "arn:${data.aws_partition.current.partition}:athena:${var.aws_region}:${data.aws_caller_identity.current.account_id}:workgroup/${var.athena_workgroup_name}"
  workflow_bucket_arns = distinct([
    "arn:aws:s3:::${var.process_bucket_name}",
    "arn:aws:s3:::${var.surface_bucket_name}",
  ])
  workflow_object_arns = distinct([
    "arn:aws:s3:::${var.process_bucket_name}/*",
    "arn:aws:s3:::${var.surface_bucket_name}/*",
  ])
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

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
    sid    = "WorkflowBucketAccess"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:ListBucket",
      "s3:PutObject",
      "s3:DeleteObject",
    ]

    resources = concat(local.workflow_bucket_arns, local.workflow_object_arns)
  }

  statement {
    sid    = "AthenaResultsBucketAccess"
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]

    resources = [
      "arn:aws:s3:::${var.athena_results_bucket_name}",
      "arn:aws:s3:::${var.athena_results_bucket_name}/*",
    ]
  }

  statement {
    sid    = "AthenaQueryExecutionAccess"
    effect = "Allow"

    actions = [
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:StartQueryExecution",
      "athena:StopQueryExecution",
    ]

    resources = [local.athena_workgroup_arn]
  }

  statement {
    sid    = "GlueCatalogAccess"
    effect = "Allow"

    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchCreatePartition",
      "glue:BatchDeletePartition",
      "glue:BatchUpdatePartition",
      "glue:CreateTable",
      "glue:DeleteTable",
      "glue:UpdateTable",
    ]

    resources = [
      "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${var.glue_database_name}",
      "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${var.glue_database_name}/*",
    ]
  }

  statement {
    sid    = "AthenaWorkGroupAccess"
    effect = "Allow"

    actions = ["athena:GetWorkGroup"]

    resources = [local.athena_workgroup_arn]
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
    merge(
      {
        name      = "dbt"
        image     = var.container_image
        essential = true
        environment = [
          {
            name  = "WORKFLOW_NAME"
            value = var.workflow_name
          },
          {
            name  = "PROCESS_BUCKET_NAME"
            value = var.process_bucket_name
          },
          {
            name  = "SURFACE_BUCKET_NAME"
            value = var.surface_bucket_name
          },
          {
            name  = "PROCESS_S3_ROOT"
            value = var.process_s3_root
          },
          {
            name  = "SURFACE_S3_ROOT"
            value = var.surface_s3_root
          },
          {
            name  = "GLUE_DATABASE_NAME"
            value = var.glue_database_name
          },
          {
            name  = "ATHENA_WORKGROUP_NAME"
            value = var.athena_workgroup_name
          },
          {
            name  = "ATHENA_RESULTS_BUCKET_NAME"
            value = var.athena_results_bucket_name
          },
          {
            name  = "AWS_REGION"
            value = var.aws_region
          },
        ]
        logConfiguration = {
          logDriver = "awslogs"
          options = {
            awslogs-group         = aws_cloudwatch_log_group.this.name
            awslogs-region        = var.aws_region
            awslogs-stream-prefix = "ecs"
          }
        }
      },
      var.command == null ? {} : { command = var.command },
    )
  ])
}
