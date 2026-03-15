locals {
  aws_region  = "us-east-2"
  aws_profile = "default"

  common_tags = {
    Project   = "data-simulator-elt"
    Owner     = "amcaughan"
    ManagedBy = "terragrunt"
  }
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
variable "extra_default_tags" {
  type    = map(string)
  default = {}
}

provider "aws" {
  region  = "${local.aws_region}"
  profile = "${local.aws_profile}"

  default_tags {
    tags = merge(${jsonencode(local.common_tags)}, var.extra_default_tags)
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
