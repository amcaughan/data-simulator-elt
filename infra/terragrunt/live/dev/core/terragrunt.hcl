include "root" {
  path = find_in_parent_folders("root.hcl")
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
