locals {
  project_slug      = replace(var.project_name, "_", "-")
  workflow_token    = "${substr(replace(var.workflow_name, "-", ""), 0, 3)}${substr(md5(var.workflow_name), 0, 5)}"
  dbt_repo_name     = "${local.project_slug}-${var.environment}-${local.workflow_token}-dbt"
  scheduler_name    = "${local.project_slug}-${var.environment}-${local.workflow_token}-sch"
  source_schedule_name = "${local.project_slug}-${var.environment}-${local.workflow_token}-si"
  standardize_schedule_name = "${local.project_slug}-${var.environment}-${local.workflow_token}-std"
  dbt_schedule_name = "${local.project_slug}-${var.environment}-${local.workflow_token}-dbt"
  source_schedule_enabled = var.ingest_schedule_expression != null
  standardize_schedule_enabled = var.standardize_schedule_expression != null
  dbt_schedule_enabled = var.dbt_schedule_expression != null
  any_schedule_enabled = local.source_schedule_enabled || local.standardize_schedule_enabled || local.dbt_schedule_enabled
}

module "storage" {
  source = "../isolated-storage"

  environment           = var.environment
  project_name          = var.project_name
  workflow_name         = var.workflow_name
  landing_bucket_name   = var.landing_bucket_name
  processed_bucket_name = var.processed_bucket_name
  marts_bucket_name     = var.marts_bucket_name
}

module "source_ingest" {
  source = "../source-ingest-job"

  environment                    = var.environment
  project_name                   = var.project_name
  workflow_name                  = var.workflow_name
  landing_bucket_name            = module.storage.landing_bucket_name
  landing_base_prefix            = var.landing_base_prefix
  landing_partition_fields_json  = var.landing_partition_fields_json
  landing_path_suffix_json       = var.landing_path_suffix_json
  source_base_url_ssm_param_name = var.source_base_url_ssm_param_name
  source_adapter                 = var.source_adapter
  source_adapter_config_json     = var.source_adapter_config_json
  aws_region                     = var.aws_region
  slice_granularity              = var.slice_granularity
  slice_selector_mode            = var.source_ingest_slice_selector_mode
  slice_pinned_at                = var.source_ingest_slice_pinned_at
  slice_range_start_at           = var.source_ingest_slice_range_start_at
  slice_range_end_at             = var.source_ingest_slice_range_end_at
  slice_relative_count           = var.source_ingest_slice_relative_count
  slice_relative_direction       = var.source_ingest_slice_relative_direction
  slice_relative_anchor_at       = var.source_ingest_slice_relative_anchor_at
  slice_alignment_policy         = var.source_ingest_slice_alignment_policy
  slice_range_policy             = var.source_ingest_slice_range_policy
  container_image                = var.source_ingest_container_image
}

module "standardize" {
  source = "../standardize-job"

  environment                    = var.environment
  project_name                   = var.project_name
  workflow_name                  = var.workflow_name
  landing_bucket_name            = module.storage.landing_bucket_name
  processed_bucket_name          = module.storage.processed_bucket_name
  landing_base_prefix            = var.landing_base_prefix
  landing_partition_fields_json  = var.landing_partition_fields_json
  landing_path_suffix_json       = var.landing_path_suffix_json
  standardize_strategy           = var.standardize_strategy
  standardize_strategy_config_json = var.standardize_strategy_config_json
  aws_region                     = var.aws_region
  landing_slice_granularity      = var.slice_granularity
  output_slice_granularity       = var.standardize_output_slice_granularity
  processed_base_prefix          = var.standardize_processed_base_prefix
  processed_partition_fields_json = var.standardize_processed_partition_fields_json
  processed_path_suffix_json     = var.standardize_processed_path_suffix_json
  slice_selector_mode            = var.standardize_slice_selector_mode
  slice_pinned_at                = var.standardize_slice_pinned_at
  slice_range_start_at           = var.standardize_slice_range_start_at
  slice_range_end_at             = var.standardize_slice_range_end_at
  slice_relative_count           = var.standardize_slice_relative_count
  slice_relative_direction       = var.standardize_slice_relative_direction
  slice_relative_anchor_at       = var.standardize_slice_relative_anchor_at
  slice_alignment_policy         = var.standardize_slice_alignment_policy
  slice_range_policy             = var.standardize_slice_range_policy
  container_image                = var.standardize_container_image
}

resource "aws_ecr_repository" "dbt" {
  name                 = local.dbt_repo_name
  image_tag_mutability = "IMMUTABLE"
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
  athena_results_bucket_name = var.athena_results_bucket_name
  container_image            = module.dbt_image.image_uri
}

data "aws_iam_policy_document" "scheduler_assume_role" {
  count = local.any_schedule_enabled ? 1 : 0

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
  count = local.any_schedule_enabled ? 1 : 0

  name               = local.scheduler_name
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume_role[0].json
}

data "aws_iam_policy_document" "scheduler" {
  count = local.any_schedule_enabled ? 1 : 0

  statement {
    sid    = "RunWorkflowTasks"
    effect = "Allow"

    actions = ["ecs:RunTask"]

    resources = [
      module.source_ingest.task_definition_arn,
      module.standardize.task_definition_arn,
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
      module.standardize.task_role_arn,
      module.standardize.execution_role_arn,
      module.dbt.task_role_arn,
      module.dbt.execution_role_arn,
    ]
  }
}

resource "aws_iam_role_policy" "scheduler" {
  count = local.any_schedule_enabled ? 1 : 0

  name   = local.scheduler_name
  role   = aws_iam_role.scheduler[0].id
  policy = data.aws_iam_policy_document.scheduler[0].json
}

resource "aws_scheduler_schedule" "source_ingest" {
  count = local.source_schedule_enabled ? 1 : 0

  name                = local.source_schedule_name
  schedule_expression = var.ingest_schedule_expression
  state               = "ENABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = var.ecs_cluster_arn
    role_arn = aws_iam_role.scheduler[0].arn

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

resource "aws_scheduler_schedule" "standardize" {
  count = local.standardize_schedule_enabled ? 1 : 0

  name                = local.standardize_schedule_name
  schedule_expression = var.standardize_schedule_expression
  state               = "ENABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = var.ecs_cluster_arn
    role_arn = aws_iam_role.scheduler[0].arn

    ecs_parameters {
      task_definition_arn = module.standardize.task_definition_arn
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
  count = local.dbt_schedule_enabled ? 1 : 0

  name                = local.dbt_schedule_name
  schedule_expression = var.dbt_schedule_expression
  state               = "ENABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = var.ecs_cluster_arn
    role_arn = aws_iam_role.scheduler[0].arn

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
