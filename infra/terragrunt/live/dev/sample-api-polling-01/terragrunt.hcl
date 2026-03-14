include "root" {
  path = find_in_parent_folders("root.hcl")
}

dependency "core" {
  config_path = "../core"

  mock_outputs = {
    ecs_cluster_arn                     = "arn:aws:ecs:us-east-2:111111111111:cluster/data-simulator-elt-dev"
    glue_database_name                  = "data_simulator_elt_dev"
    athena_workgroup_name               = "data-simulator-elt-dev"
    athena_results_bucket_name          = "data-simulator-elt-dev-athena-results-111111111111-us-east-2"
    network_private_subnet_ids          = ["subnet-placeholder"]
    network_security_group_id           = "sg-placeholder"
    source_ingest_image_uri             = "111111111111.dkr.ecr.us-east-2.amazonaws.com/data-simulator-elt-dev-source-ingest:sha-placeholder"
    standardize_image_uri               = "111111111111.dkr.ecr.us-east-2.amazonaws.com/data-simulator-elt-dev-standardize:sha-placeholder"
  }

  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan"]
}

terraform {
  source = "${get_repo_root()}/infra/terragrunt//modules/scheduled-workflow"
}

inputs = {
  environment                      = "dev"
  project_name                     = "data-simulator-elt"
  workflow_name                    = "sample-api-polling-01"
  ecs_cluster_arn                  = dependency.core.outputs.ecs_cluster_arn
  network_private_subnet_ids       = dependency.core.outputs.network_private_subnet_ids
  network_security_group_id        = dependency.core.outputs.network_security_group_id
  glue_database_name               = dependency.core.outputs.glue_database_name
  athena_workgroup_name            = dependency.core.outputs.athena_workgroup_name
  athena_results_bucket_name       = dependency.core.outputs.athena_results_bucket_name
  source_base_url_ssm_param_name   = "/services/data-simulator-api/dev/private_api_invoke_url"
  ingest_schedule_expression       = null
  standardize_schedule_expression  = null
  dbt_schedule_expression          = null
  source_adapter                   = "simulator_api"
  source_adapter_config_json       = jsonencode({
    preset_id      = "transaction_benchmark"
    row_count      = 250
    seed_strategy  = "derived"
    request_overrides = {}
  })
  standardize_strategy             = "simulator_api"
  standardize_strategy_config_json = jsonencode({
    preset_id = "transaction_benchmark"
  })
  slice_granularity                = "hour"
  dbt_source_dir                   = "${get_repo_root()}/containers/workflows/sample-api-polling-01/dbt"
  source_ingest_container_image    = dependency.core.outputs.source_ingest_image_uri
  standardize_container_image      = dependency.core.outputs.standardize_image_uri
}
