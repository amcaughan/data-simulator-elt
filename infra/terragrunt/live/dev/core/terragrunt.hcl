include "root" {
  path = find_in_parent_folders("root.hcl")
}

terraform {
  source = "${get_repo_root()}/infra/terragrunt/modules/elt-core"
}

inputs = {
  environment                                 = "dev"
  project_name                                = "data-simulator-elt"
  network_vpc_id_ssm_param_name               = "/network/dev/vpc/vpc_id"
  network_private_subnet_ids_ssm_param_name   = "/network/dev/vpc/private_subnet_ids"
  network_shared_security_group_ssm_param_name = "/network/dev/vpc/shared_workload_security_group_id"
  publish_ssm_parameters                      = true
  publish_runtime_images                      = true
  jobs_build_context_dir                      = "${get_repo_root()}/jobs"
  jobs_requirements_file                      = "${get_repo_root()}/jobs/requirements.txt"
  common_source_dir                           = "${get_repo_root()}/jobs/common"
  source_ingest_source_dir                    = "${get_repo_root()}/jobs/source_ingest"
  source_ingest_dockerfile_path               = "${get_repo_root()}/jobs/source_ingest/Dockerfile"
  standardize_source_dir                      = "${get_repo_root()}/jobs/standardize"
  standardize_dockerfile_path                 = "${get_repo_root()}/jobs/standardize/Dockerfile"
  dbt_source_dir                              = "${get_repo_root()}/jobs/dbt"
  dbt_requirements_file                       = "${get_repo_root()}/jobs/dbt/requirements.txt"
  dbt_dockerfile_path                         = "${get_repo_root()}/jobs/dbt/Dockerfile"
  ssm_prefix                                  = "/services/data-simulator-elt/dev/core"
}
