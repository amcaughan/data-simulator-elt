include "root" {
  path   = find_in_parent_folders("root.hcl")
  expose = true
}

locals {
  cleanup_tags = {
    # Weekly cleanup keeps this sandbox affordable. In a real production system,
    # these resources would usually stay up until intentionally retired.
    auto_cleanup     = "true"
    cleanup_schedule = "weekly"
    created_on       = run_cmd("date", "-u", "+%Y-%m-%d")
  }
}

generate "provider" {
  path      = "provider.tf"
  if_exists = "overwrite_terragrunt"

  contents = <<EOF
provider "aws" {
  region  = "${include.root.locals.aws_region}"
  profile = "${include.root.locals.aws_profile}"

  default_tags {
    tags = ${jsonencode(merge(include.root.locals.common_tags, local.cleanup_tags))}
  }
}
EOF
}

terraform {
  source = "${get_repo_root()}/infra/terragrunt//modules/elt-core"
}

inputs = {
  environment                                 = "dev"
  project_name                                = "data-simulator-elt"
  network_vpc_id_ssm_param_name               = "/network/dev/vpc/vpc_id"
  network_private_subnet_ids_ssm_param_name   = "/network/dev/vpc/private_subnet_ids"
  network_shared_security_group_ssm_param_name = "/network/dev/vpc/shared_workload_security_group_id"
  publish_ssm_parameters                      = true
  publish_runtime_images                      = true
  shared_containers_build_context_dir         = "${get_repo_root()}/containers/shared"
  shared_common_container_source_dir          = "${get_repo_root()}/containers/shared/common"
  source_ingest_container_source_dir          = "${get_repo_root()}/containers/shared/source_ingest"
  standardize_container_source_dir            = "${get_repo_root()}/containers/shared/standardize"
  ssm_prefix                                  = "/services/data-simulator-elt/dev/core"
}
