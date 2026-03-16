data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  workflow_slug = trimsuffix(trimprefix(replace(var.workflow_name, "_", "-"), "sample-"), "-")
  bucket_prefix = "elt-${local.workflow_slug}-${var.environment}"

  requested_storage_locations = {
    for location_name, config in var.storage_locations : location_name => {
      bucket_name = try(trimspace(config.bucket_name), "") == "" ? null : trimspace(config.bucket_name)
      prefix      = try(trimspace(config.prefix), "") == "" ? null : trim(try(trimspace(config.prefix), ""), "/")
    }
  }

  default_bucket_names = {
    for location_name in keys(local.requested_storage_locations) :
    location_name => "${local.bucket_prefix}-${location_name}-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.region}"
  }

  resolved_storage_locations = {
    for location_name, config in local.requested_storage_locations : location_name => {
      bucket_name = coalesce(config.bucket_name, local.default_bucket_names[location_name])
      prefix      = config.prefix
      s3_root     = config.prefix == null ? "s3://${coalesce(config.bucket_name, local.default_bucket_names[location_name])}/" : "s3://${coalesce(config.bucket_name, local.default_bucket_names[location_name])}/${config.prefix}/"
    }
  }

  unique_bucket_names = toset([
    for config in values(local.resolved_storage_locations) : config.bucket_name
  ])
}

resource "aws_s3_bucket" "this" {
  for_each = {
    for bucket_name in local.unique_bucket_names : bucket_name => bucket_name
  }

  bucket        = each.value
  force_destroy = var.force_destroy_buckets
}

resource "aws_s3_bucket_versioning" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "aws_iam_policy_document" "tls_only" {
  for_each = aws_s3_bucket.this

  statement {
    sid    = "DenyInsecureTransport"
    effect = "Deny"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = ["s3:*"]

    resources = [
      each.value.arn,
      "${each.value.arn}/*",
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id
  policy = data.aws_iam_policy_document.tls_only[each.key].json
}
