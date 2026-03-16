include "root" {
  path = find_in_parent_folders("root.hcl")
}

locals {
  workflow_release_manifest_path = "${get_repo_root()}/build/releases/dev/sample-stream-events-01.json"
  workflow_release_manifest      = fileexists(local.workflow_release_manifest_path) ? jsondecode(file(local.workflow_release_manifest_path)) : {}
}

dependency "core" {
  config_path = "../core"

  mock_outputs = {
    ecs_cluster_arn            = "arn:aws:ecs:us-east-2:111111111111:cluster/data-simulator-elt-dev"
    glue_database_name         = "data_simulator_elt_dev"
    athena_workgroup_name      = "data-simulator-elt-dev"
    athena_results_bucket_name = "data-simulator-elt-dev-athena-results-111111111111-us-east-2"
    network_private_subnet_ids = ["subnet-placeholder"]
    network_security_group_id  = "sg-placeholder"
  }

  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan"]
}

terraform {
  source = "${get_repo_root()}/infra/terragrunt//modules/streaming-workflow"
}

inputs = {
  environment = "dev"
  extra_default_tags = {
    # Weekly cleanup keeps this sandbox affordable. In a real production system,
    # these resources would usually stay up until intentionally retired.
    auto_cleanup     = "true"
    cleanup_schedule = "weekly"
    # Intentional here: the janitor treats apply time as "last touched" time.
    created_on = run_cmd("date", "-u", "+%Y-%m-%d")
  }
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
  # Keep the demo stream quiet until we intentionally run it by hand.
  stream_schedule_expression     = null
  dbt_schedule_expression        = null
  dbt_container_image            = try(local.workflow_release_manifest.dbt_image_uri, null)
  stream_emitter_container_image = try(local.workflow_release_manifest.stream_emitter_image_uri, null)
}
