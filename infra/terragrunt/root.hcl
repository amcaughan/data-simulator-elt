locals {
  stack_path_parts     = split("/", path_relative_to_include())
  environment_name     = length(local.stack_path_parts) > 1 ? local.stack_path_parts[1] : ""
  stack_name           = length(local.stack_path_parts) > 2 ? local.stack_path_parts[2] : ""
  cleanup_tags_enabled = local.environment_name == "dev"
  cleanup_tags = local.cleanup_tags_enabled ? {
    # Weekly cleanup keeps this sandbox affordable. In a real production system,
    # these resources would usually stay up until intentionally retired.
    auto_cleanup     = "true"
    cleanup_schedule = "weekly"
    cleanup_ttl      = "7d"
    created_on       = run_cmd("date", "-u", "+%Y-%m-%d")
  } : {}
  aws_region  = "us-east-2"
  aws_profile = "default"

  common_tags = merge(
    {
      Project     = "data-simulator-elt"
      Owner       = "amcaughan"
      ManagedBy   = "terragrunt"
      Environment = local.environment_name
      Stack       = local.stack_name
    },
    local.cleanup_tags,
  )
}

remote_state {
  backend = "s3"

  config = {
    bucket       = "amcaughan-tf-state-us-east-2"
    key          = "${path_relative_to_include()}/terraform.tfstate"
    region       = local.aws_region
    encrypt      = true
    use_lockfile = true
  }
}

generate "provider" {
  path      = "provider.tf"
  if_exists = "overwrite_terragrunt"

  contents = <<EOF
provider "aws" {
  region  = "${local.aws_region}"
  profile = "${local.aws_profile}"

  default_tags {
    tags = ${jsonencode(local.common_tags)}
  }
}
EOF
}

generate "backend_stub" {
  path      = "backend.tf"
  if_exists = "overwrite_terragrunt"

  contents = <<EOF
terraform {
  backend "s3" {}
}
EOF
}
