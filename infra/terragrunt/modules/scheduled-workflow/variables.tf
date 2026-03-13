variable "environment" {
  type = string
}

variable "project_name" {
  type    = string
  default = "data-simulator-elt"
}

variable "aws_region" {
  type    = string
  default = "us-east-2"
}

variable "workflow_name" {
  type = string
}

variable "ecs_cluster_arn" {
  type = string
}

variable "network_private_subnet_ids" {
  type = list(string)
}

variable "network_security_group_id" {
  type = string
}

variable "glue_database_name" {
  type = string
}

variable "athena_workgroup_name" {
  type = string
}

variable "source_base_url_ssm_param_name" {
  type = string
}

variable "ingest_schedule_expression" {
  type = string
}

variable "standardize_schedule_expression" {
  type = string
}

variable "dbt_schedule_expression" {
  type    = string
  default = null
}

variable "dbt_source_dir" {
  type = string
}

variable "source_adapter" {
  type    = string
  default = "simulator_api"
}

variable "source_adapter_config_json" {
  type = string
}

variable "landing_base_prefix" {
  type    = string
  default = null
}

variable "landing_partition_fields_json" {
  type    = string
  default = null
}

variable "landing_path_suffix_json" {
  type    = string
  default = null
}

variable "slice_granularity" {
  type    = string
  default = "day"
}

variable "source_ingest_mode" {
  type    = string
  default = "live_hit"
}

variable "source_ingest_logical_date" {
  type    = string
  default = null
}

variable "source_ingest_start_at" {
  type    = string
  default = null
}

variable "source_ingest_end_at" {
  type    = string
  default = null
}

variable "source_ingest_backfill_count" {
  type    = number
  default = null
}

variable "source_ingest_container_image" {
  type = string
}

variable "standardize_container_image" {
  type = string
}

variable "standardize_mode" {
  type    = string
  default = "live_hit"
}

variable "standardize_logical_date" {
  type    = string
  default = null
}

variable "standardize_start_at" {
  type    = string
  default = null
}

variable "standardize_end_at" {
  type    = string
  default = null
}

variable "standardize_backfill_count" {
  type    = number
  default = null
}

variable "standardize_output_slice_granularity" {
  type    = string
  default = "day"
}

variable "standardize_processed_output_prefix" {
  type    = string
  default = "raw"
}
