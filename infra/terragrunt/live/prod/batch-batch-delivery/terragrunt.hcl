include "root" {
  path = find_in_parent_folders("root.hcl")
}

dependency "core" {
  config_path = "../core"

  mock_outputs = {
    environment                         = "prod"
    ecs_cluster_name                    = "data-simulator-elt-prod"
    ecs_cluster_arn                     = "arn:aws:ecs:us-east-2:111111111111:cluster/data-simulator-elt-prod"
    glue_database_name                  = "data_simulator_elt_prod"
    athena_workgroup_name               = "data-simulator-elt-prod"
    network_vpc_id                      = "vpc-placeholder"
    network_private_subnet_ids          = ["subnet-placeholder"]
    network_security_group_id           = "sg-placeholder"
    scheduled_ingest_ecr_repository_url = "111111111111.dkr.ecr.us-east-2.amazonaws.com/data-simulator-elt-prod-scheduled-ingest"
    dbt_ecr_repository_url              = "111111111111.dkr.ecr.us-east-2.amazonaws.com/data-simulator-elt-prod-dbt"
  }

  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan"]
}

terraform {
  source = "${get_repo_root()}/infra/terragrunt/modules/scheduled-workflow"
}

inputs = {
  environment                      = "prod"
  project_name                     = "data-simulator-elt"
  workflow_name                    = "batch-batch-delivery"
  ecs_cluster_arn                  = dependency.core.outputs.ecs_cluster_arn
  network_private_subnet_ids       = dependency.core.outputs.network_private_subnet_ids
  network_security_group_id        = dependency.core.outputs.network_security_group_id
  glue_database_name               = dependency.core.outputs.glue_database_name
  athena_workgroup_name            = dependency.core.outputs.athena_workgroup_name
  simulator_api_url_ssm_param_name = "/services/data-simulator-api/prod/private_api_invoke_url"
  ingest_schedule_expression     = "cron(15 5 * * ? *)"
  dbt_schedule_expression        = "cron(45 5 * * ? *)"
  preset_id                      = "batch_delivery_benchmark"
  row_count                      = 5000
  ingest_container_image         = "${dependency.core.outputs.scheduled_ingest_ecr_repository_url}:latest"
  dbt_container_image            = "${dependency.core.outputs.dbt_ecr_repository_url}:latest"
}
