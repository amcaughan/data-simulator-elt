include "root" {
  path = find_in_parent_folders("root.hcl")
}

dependency "core" {
  config_path = "../core"

  mock_outputs = {
    ecs_cluster_arn            = "arn:aws:ecs:us-east-2:111111111111:cluster/data-simulator-elt-prod"
    glue_database_name         = "data_simulator_elt_prod"
    athena_workgroup_name      = "data-simulator-elt-prod"
    athena_results_bucket_name = "data-simulator-elt-prod-athena-results-111111111111-us-east-2"
    network_private_subnet_ids = ["subnet-placeholder"]
    network_security_group_id  = "sg-placeholder"
  }

  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan"]
}

terraform {
  source = "${get_repo_root()}/infra/terragrunt//modules/streaming-workflow"
}

inputs = {
  environment                      = "prod"
  project_name                     = "data-simulator-elt"
  workflow_name                    = "sample-stream-events-01"
  ecs_cluster_arn                  = dependency.core.outputs.ecs_cluster_arn
  network_private_subnet_ids       = dependency.core.outputs.network_private_subnet_ids
  network_security_group_id        = dependency.core.outputs.network_security_group_id
  glue_database_name               = dependency.core.outputs.glue_database_name
  athena_workgroup_name            = dependency.core.outputs.athena_workgroup_name
  athena_results_bucket_name       = dependency.core.outputs.athena_results_bucket_name
  simulator_api_url_ssm_param_name = "/services/data-simulator-api/prod/private_api_invoke_url"
  preset_id                        = "iot_sensor_benchmark"
  emission_rate_per_minute         = 60
  # Keep the demo stream quiet until we intentionally run it by hand.
  stream_schedule_expression = null
  dbt_schedule_expression    = null
  dbt_source_dir             = "${get_repo_root()}/containers/workflows/sample-stream-events-01/dbt"
  stream_emitter_source_dir  = "${get_repo_root()}/containers/workflows/sample-stream-events-01/stream_emitter"
}
