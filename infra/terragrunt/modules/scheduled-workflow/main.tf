locals {
  project_slug = replace(var.project_name, "_", "-")
}

module "storage" {
  source = "../isolated-storage"

  environment   = var.environment
  project_name  = var.project_name
  workflow_name = var.workflow_name
}

module "source_ingest" {
  source = "../source-ingest-job"

  environment                      = var.environment
  project_name                     = var.project_name
  workflow_name                    = var.workflow_name
  landing_bucket_name              = module.storage.landing_bucket_name
  simulator_api_url_ssm_param_name = var.simulator_api_url_ssm_param_name
  source_adapter                   = var.source_adapter
  preset_id                        = var.preset_id
  row_count                        = var.row_count
  partition_granularity            = var.partition_granularity
  mode                             = var.source_ingest_mode
  logical_date                     = var.source_ingest_logical_date
  start_at                         = var.source_ingest_start_at
  end_at                           = var.source_ingest_end_at
  backfill_days                    = var.source_ingest_backfill_days
  seed_strategy                    = var.source_ingest_seed_strategy
  fixed_seed                       = var.source_ingest_fixed_seed
  request_overrides_json           = var.source_ingest_request_overrides_json
  container_image                  = var.source_ingest_container_image
}

module "dbt" {
  source = "../dbt-job"

  environment                = var.environment
  project_name               = var.project_name
  workflow_name              = var.workflow_name
  processed_bucket_name      = module.storage.processed_bucket_name
  analytics_bucket_name      = module.storage.analytics_bucket_name
  glue_database_name         = var.glue_database_name
  athena_workgroup_name      = var.athena_workgroup_name
  container_image            = var.dbt_container_image
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
      module.source_ingest.task_definition_arn,
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
      module.source_ingest.task_role_arn,
      module.source_ingest.execution_role_arn,
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

resource "aws_scheduler_schedule" "source_ingest" {
  name                = "${local.project_slug}-${var.environment}-${var.workflow_name}-source-ingest"
  schedule_expression = var.ingest_schedule_expression
  state               = "ENABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = var.ecs_cluster_arn
    role_arn = aws_iam_role.scheduler.arn

    ecs_parameters {
      task_definition_arn = module.source_ingest.task_definition_arn
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
