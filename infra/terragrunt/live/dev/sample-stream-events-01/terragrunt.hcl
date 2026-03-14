include "root" {
  path = find_in_parent_folders("root.hcl")
}

dependency "core" {
  config_path = "../core"

  mock_outputs = {
    ecs_cluster_arn                   = "arn:aws:ecs:us-east-2:111111111111:cluster/data-simulator-elt-dev"
    glue_database_name                = "data_simulator_elt_dev"
    athena_workgroup_name             = "data-simulator-elt-dev"
    athena_results_bucket_name        = "data-simulator-elt-dev-athena-results-111111111111-us-east-2"
    network_private_subnet_ids        = ["subnet-placeholder"]
    network_security_group_id         = "sg-placeholder"
    stream_emitter_ecr_repository_url = "111111111111.dkr.ecr.us-east-2.amazonaws.com/data-simulator-elt-dev-stream-emitter"
  }

  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan"]
}

terraform {
  source = "${get_repo_root()}/infra/terragrunt//modules/streaming-workflow"
}

inputs = {
  environment                      = "dev"
  project_name                     = "data-simulator-elt"
  workflow_name                    = "sample-stream-events-01"
  ecs_cluster_arn                  = dependency.core.outputs.ecs_cluster_arn
  network_private_subnet_ids       = dependency.core.outputs.network_private_subnet_ids
  network_security_group_id        = dependency.core.outputs.network_security_group_id
  glue_database_name               = dependency.core.outputs.glue_database_name
  athena_workgroup_name            = dependency.core.outputs.athena_workgroup_name
  athena_results_bucket_name       = dependency.core.outputs.athena_results_bucket_name
  simulator_api_url_ssm_param_name = "/services/data-simulator-api/dev/private_api_invoke_url"
  preset_id                        = "iot_sensor_benchmark"
  emission_rate_per_minute         = 60
  stream_schedule_expression       = "rate(1 minute)"
  dbt_schedule_expression          = "cron(10 * * * ? *)"
  dbt_source_dir                   = "${get_repo_root()}/containers/workflows/sample-stream-events-01/dbt"
  stream_emitter_container_image   = "${dependency.core.outputs.stream_emitter_ecr_repository_url}:latest"
}
