data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  workflow_slug = trimsuffix(trimprefix(replace(var.workflow_name, "_", "-"), "sample-"), "-")
  bucket_prefix = "elt-${local.workflow_slug}-${var.environment}"
  bucket_names = {
    landing   = coalesce(var.landing_bucket_name, "${local.bucket_prefix}-landing-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.region}")
    processed = coalesce(var.processed_bucket_name, "${local.bucket_prefix}-proc-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.region}")
    marts     = coalesce(var.marts_bucket_name, "${local.bucket_prefix}-mart-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.region}")
  }
}

resource "aws_s3_bucket" "this" {
  for_each = local.bucket_names

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
