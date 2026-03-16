include "root" {
  path = find_in_parent_folders("root.hcl")
}

locals {
  core_release_manifest_path     = "${get_repo_root()}/build/releases/dev/core.json"
  core_release_manifest          = fileexists(local.core_release_manifest_path) ? jsondecode(file(local.core_release_manifest_path)) : {}
  workflow_release_manifest_path = "${get_repo_root()}/build/releases/dev/sample-file-delivery-01.json"
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
    source_ingest_image_uri    = "111111111111.dkr.ecr.us-east-2.amazonaws.com/data-simulator-elt-dev-source-ingest:sha-placeholder"
    standardize_image_uri      = "111111111111.dkr.ecr.us-east-2.amazonaws.com/data-simulator-elt-dev-standardize:sha-placeholder"
  }

  mock_outputs_allowed_terraform_commands = ["init", "validate", "plan"]
}

terraform {
  source = "${get_repo_root()}/infra/terragrunt//modules/scheduled-workflow"
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
  project_name  = "data-simulator-elt"
  workflow_name = "sample-file-delivery-01"
  storage_locations = {
    ingest  = {}
    process = {}
    surface = {}
  }
  ecs_cluster_arn                 = dependency.core.outputs.ecs_cluster_arn
  network_private_subnet_ids      = dependency.core.outputs.network_private_subnet_ids
  network_security_group_id       = dependency.core.outputs.network_security_group_id
  glue_database_name              = dependency.core.outputs.glue_database_name
  athena_workgroup_name           = dependency.core.outputs.athena_workgroup_name
  athena_results_bucket_name      = dependency.core.outputs.athena_results_bucket_name
  source_base_url_ssm_param_name  = "/services/data-simulator-api/dev/private_api_invoke_url"
  ingest_schedule_expression      = null
  standardize_schedule_expression = null
  dbt_schedule_expression         = null
  source_adapter                  = "simulator_batch_delivery"
  source_adapter_config_json = jsonencode({
    preset_id         = "batch_delivery_benchmark"
    row_count         = 2500
    seed_strategy     = "derived"
    request_overrides = {}
    deliveries = [
      {
        source_system_id = "location_1"
        feed_type        = "member_snapshot"
        object_name      = "location_1.csv"
      },
      {
        source_system_id = "location_2"
        feed_type        = "member_snapshot"
        object_name      = "location_2.csv"
      },
    ]
  })
  standardize_strategy = "batch_delivery_csv"
  standardize_strategy_config_json = jsonencode({
    preset_id = "batch_delivery_benchmark"
  })
  slice_granularity             = "day"
  dbt_container_image           = try(local.workflow_release_manifest.dbt_image_uri, null)
  source_ingest_container_image = coalesce(try(local.core_release_manifest.source_ingest_image_uri, null), try(dependency.core.outputs.source_ingest_image_uri, null))
  standardize_container_image   = coalesce(try(local.core_release_manifest.standardize_image_uri, null), try(dependency.core.outputs.standardize_image_uri, null))
}
