include "root" {
  path = find_in_parent_folders("root.hcl")
}

terraform {
  source = "${get_repo_root()}/infra/terragrunt/modules/elt-core"
}

inputs = {
  environment                                 = "prod"
  project_name                                = "data-simulator-elt"
  network_vpc_id_ssm_param_name               = "/network/prod/vpc/vpc_id"
  network_private_subnet_ids_ssm_param_name   = "/network/prod/vpc/private_subnet_ids"
  network_shared_security_group_ssm_param_name = "/network/prod/vpc/shared_workload_security_group_id"
  publish_ssm_parameters                      = true
  publish_runtime_images                      = true
  jobs_build_context_dir                      = "${get_repo_root()}/jobs"
  common_source_dir                           = "${get_repo_root()}/jobs/common"
  source_ingest_source_dir                    = "${get_repo_root()}/jobs/source_ingest"
  standardize_source_dir                      = "${get_repo_root()}/jobs/standardize"
  ssm_prefix                                  = "/services/data-simulator-elt/prod/core"
}
