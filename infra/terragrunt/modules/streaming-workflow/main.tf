locals {
  project_slug        = replace(var.project_name, "_", "-")
  stream_name         = "${local.project_slug}-${var.environment}-${var.workflow_name}"
  firehose_name       = "${local.project_slug}-${var.environment}-${var.workflow_name}-processed"
  firehose_log_group  = "/aws/kinesisfirehose/${local.firehose_name}"
  dbt_repo_name       = "${local.project_slug}-${var.environment}-${var.workflow_name}-dbt"
}

module "storage" {
  source = "../isolated-storage"

  environment   = var.environment
  project_name  = var.project_name
  workflow_name = var.workflow_name
}

resource "aws_kinesis_stream" "this" {
  name = local.stream_name

  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }
}

resource "aws_cloudwatch_log_group" "firehose" {
  name              = local.firehose_log_group
  retention_in_days = 14
}

resource "aws_cloudwatch_log_stream" "firehose" {
  name           = "delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

data "aws_iam_policy_document" "firehose_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "firehose" {
  name               = "${local.firehose_name}-role"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume_role.json
}

data "aws_iam_policy_document" "firehose" {
  statement {
    sid    = "ReadFromKinesis"
    effect = "Allow"

    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:ListShards",
      "kinesis:SubscribeToShard",
    ]

    resources = [aws_kinesis_stream.this.arn]
  }

  statement {
    sid    = "WriteToS3"
    effect = "Allow"

    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject",
    ]

    resources = [
      "arn:aws:s3:::${module.storage.processed_bucket_name}",
      "arn:aws:s3:::${module.storage.processed_bucket_name}/*",
    ]
  }

  statement {
    sid    = "FirehoseLogging"
    effect = "Allow"

    actions = ["logs:PutLogEvents"]

    resources = ["${aws_cloudwatch_log_group.firehose.arn}:log-stream:${aws_cloudwatch_log_stream.firehose.name}"]
  }
}

resource "aws_iam_role_policy" "firehose" {
  name   = "${local.firehose_name}-role"
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.firehose.json
}

resource "aws_kinesis_firehose_delivery_stream" "this" {
  name        = local.firehose_name
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.this.arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn           = aws_iam_role.firehose.arn
    bucket_arn         = "arn:aws:s3:::${module.storage.processed_bucket_name}"
    prefix             = "events/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    error_output_prefix = "errors/type=!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    buffering_interval = 60
    buffering_size     = 64
    compression_format = "GZIP"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose.name
    }
  }
}

module "stream_emitter" {
  source = "../stream-emitter-job"

  environment                    = var.environment
  project_name                   = var.project_name
  workflow_name                  = var.workflow_name
  simulator_api_url_ssm_param_name = var.simulator_api_url_ssm_param_name
  preset_id                      = var.preset_id
  emission_rate_per_minute       = var.emission_rate_per_minute
  stream_name                    = aws_kinesis_stream.this.name
  stream_arn                     = aws_kinesis_stream.this.arn
  container_image                = var.stream_emitter_container_image
}

resource "aws_ecr_repository" "dbt" {
  name                 = local.dbt_repo_name
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "dbt" {
  repository = aws_ecr_repository.dbt.name
  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep only the newest tagged workflow dbt image"
        selection = {
          tagStatus     = "tagged"
          tagPrefixList = ["sha-"]
          countType     = "imageCountMoreThan"
          countNumber   = 1
        }
        action = {
          type = "expire"
        }
      },
      {
        rulePriority = 2
        description  = "Expire untagged images"
        selection = {
          tagStatus   = "untagged"
          countType   = "imageCountMoreThan"
          countNumber = 1
        }
        action = {
          type = "expire"
        }
      },
    ]
  })
}

module "dbt_image" {
  source = "../container-image"

  aws_region         = var.aws_region
  repository_url     = aws_ecr_repository.dbt.repository_url
  runtime_source_dir = var.dbt_source_dir
  build_context_dir  = var.dbt_source_dir
}

module "dbt" {
  source = "../dbt-job"

  environment                = var.environment
  project_name               = var.project_name
  workflow_name              = var.workflow_name
  processed_bucket_name      = module.storage.processed_bucket_name
  marts_bucket_name          = module.storage.marts_bucket_name
  glue_database_name         = var.glue_database_name
  athena_workgroup_name      = var.athena_workgroup_name
  container_image            = module.dbt_image.image_uri
}

data "aws_iam_policy_document" "scheduler_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${local.project_slug}-${var.environment}-${var.workflow_name}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume_role.json
}

data "aws_iam_policy_document" "scheduler" {
  statement {
    sid    = "RunWorkflowTasks"
    effect = "Allow"

    actions = ["ecs:RunTask"]

    resources = [
      module.stream_emitter.task_definition_arn,
      module.dbt.task_definition_arn,
    ]

    condition {
      test     = "ArnEquals"
      variable = "ecs:cluster"
      values   = [var.ecs_cluster_arn]
    }
  }

  statement {
    sid    = "PassWorkflowRoles"
    effect = "Allow"

    actions = ["iam:PassRole"]

    resources = [
      module.stream_emitter.task_role_arn,
      module.stream_emitter.execution_role_arn,
      module.dbt.task_role_arn,
      module.dbt.execution_role_arn,
    ]
  }
}

resource "aws_iam_role_policy" "scheduler" {
  name   = "${local.project_slug}-${var.environment}-${var.workflow_name}-scheduler"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler.json
}

resource "aws_scheduler_schedule" "stream_emitter" {
  name                = "${local.project_slug}-${var.environment}-${var.workflow_name}-emitter"
  schedule_expression = var.stream_schedule_expression
  state               = "ENABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = var.ecs_cluster_arn
    role_arn = aws_iam_role.scheduler.arn

    ecs_parameters {
      task_definition_arn = module.stream_emitter.task_definition_arn
      launch_type         = "FARGATE"
      task_count          = 1
      platform_version    = "LATEST"

      network_configuration {
        subnets          = var.network_private_subnet_ids
        security_groups  = [var.network_security_group_id]
        assign_public_ip = false
      }
    }
  }
}

resource "aws_scheduler_schedule" "dbt" {
  count = var.dbt_schedule_expression == null ? 0 : 1

  name                = "${local.project_slug}-${var.environment}-${var.workflow_name}-dbt"
  schedule_expression = var.dbt_schedule_expression
  state               = "ENABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = var.ecs_cluster_arn
    role_arn = aws_iam_role.scheduler.arn

    ecs_parameters {
      task_definition_arn = module.dbt.task_definition_arn
      launch_type         = "FARGATE"
      task_count          = 1
      platform_version    = "LATEST"

      network_configuration {
        subnets          = var.network_private_subnet_ids
        security_groups  = [var.network_security_group_id]
        assign_public_ip = false
      }
    }
  }
}
