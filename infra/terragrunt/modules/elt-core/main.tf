data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

data "aws_ssm_parameter" "network_vpc_id" {
  name = var.network_vpc_id_ssm_param_name
}

data "aws_ssm_parameter" "network_private_subnet_ids" {
  name = var.network_private_subnet_ids_ssm_param_name
}

data "aws_ssm_parameter" "network_shared_security_group_id" {
  name = var.network_shared_security_group_ssm_param_name
}

locals {
  project_slug               = replace(var.project_name, "_", "-")
  marts_database_name        = replace("${var.project_name}_${var.environment}", "-", "_")
  athena_results_bucket_name = "${local.project_slug}-${var.environment}-athena-results-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.name}"
  ecr_repositories = {
    source_ingest  = "${local.project_slug}-${var.environment}-source-ingest"
    standardize    = "${local.project_slug}-${var.environment}-standardize"
    dbt            = "${local.project_slug}-${var.environment}-dbt"
    stream_emitter = "${local.project_slug}-${var.environment}-stream-emitter"
  }
}

resource "aws_ecs_cluster" "this" {
  name = "${local.project_slug}-${var.environment}"
}

resource "aws_s3_bucket" "athena_results" {
  bucket        = local.athena_results_bucket_name
  force_destroy = var.force_destroy_stateful_resources
}

resource "aws_s3_bucket_versioning" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "aws_iam_policy_document" "athena_results_tls_only" {
  statement {
    sid    = "DenyInsecureTransport"
    effect = "Deny"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = ["s3:*"]

    resources = [
      aws_s3_bucket.athena_results.arn,
      "${aws_s3_bucket.athena_results.arn}/*",
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  policy = data.aws_iam_policy_document.athena_results_tls_only.json
}

resource "aws_athena_workgroup" "this" {
  name = "${local.project_slug}-${var.environment}"

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/query-results/"
    }
  }
}

resource "aws_athena_database" "this" {
  name          = local.marts_database_name
  bucket        = aws_s3_bucket.athena_results.bucket
  force_destroy = var.force_destroy_stateful_resources
}

resource "aws_ecr_repository" "this" {
  for_each = local.ecr_repositories

  name                 = each.value
  image_tag_mutability = "MUTABLE"
  force_delete         = var.force_destroy_stateful_resources

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ssm_parameter" "ecs_cluster_name" {
  count = var.publish_ssm_parameters ? 1 : 0

  name  = "${var.ssm_prefix}/ecs_cluster_name"
  type  = "String"
  value = aws_ecs_cluster.this.name
}

resource "aws_ssm_parameter" "glue_database_name" {
  count = var.publish_ssm_parameters ? 1 : 0

  name  = "${var.ssm_prefix}/glue_database_name"
  type  = "String"
  value = aws_athena_database.this.name
}

resource "aws_ssm_parameter" "athena_workgroup_name" {
  count = var.publish_ssm_parameters ? 1 : 0

  name  = "${var.ssm_prefix}/athena_workgroup_name"
  type  = "String"
  value = aws_athena_workgroup.this.name
}

resource "aws_ssm_parameter" "athena_results_bucket_name" {
  count = var.publish_ssm_parameters ? 1 : 0

  name  = "${var.ssm_prefix}/athena_results_bucket_name"
  type  = "String"
  value = aws_s3_bucket.athena_results.bucket
}

resource "aws_ssm_parameter" "ecr_repository_url" {
  for_each = var.publish_ssm_parameters ? aws_ecr_repository.this : {}

  name  = "${var.ssm_prefix}/ecr/${each.key}/repository_url"
  type  = "String"
  value = each.value.repository_url
}
