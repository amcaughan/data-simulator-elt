include "root" {
  path = find_in_parent_folders("root.hcl")
}

terraform {
  source = "${get_repo_root()}/infra/terragrunt//modules/elt-core"
}

inputs = {
  environment = "dev"
  extra_default_tags = {
    # Weekly cleanup keeps this sandbox affordable. In a real production system,
    # these resources would usually stay up until intentionally retired.
    auto_cleanup     = "true"
    cleanup_schedule = "weekly"
    # Intentional here: the janitor treats apply time as "last touched" time.
    created_on = run_cmd("--terragrunt-quiet", "date", "-u", "+%Y-%m-%d")
  }
  project_name                                 = "data-simulator-elt"
  network_vpc_id_ssm_param_name                = "/network/dev/vpc/vpc_id"
  network_private_subnet_ids_ssm_param_name    = "/network/dev/vpc/private_subnet_ids"
  network_shared_security_group_ssm_param_name = "/network/dev/vpc/shared_workload_security_group_id"
  publish_ssm_parameters                       = true
  ssm_prefix                                   = "/services/data-simulator-elt/dev/core"
}
