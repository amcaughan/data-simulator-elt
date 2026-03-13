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

variable "standardize_strategy" {
  type = string
}

variable "standardize_strategy_config_json" {
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

variable "source_ingest_slice_selector_mode" {
  type    = string
  default = "current"
}

variable "source_ingest_slice_pinned_at" {
  type    = string
  default = null
}

variable "source_ingest_slice_range_start_at" {
  type    = string
  default = null
}

variable "source_ingest_slice_range_end_at" {
  type    = string
  default = null
}

variable "source_ingest_slice_relative_count" {
  type    = number
  default = null
}

variable "source_ingest_slice_relative_direction" {
  type    = string
  default = null
}

variable "source_ingest_slice_relative_anchor_at" {
  type    = string
  default = null
}

variable "source_ingest_slice_alignment_policy" {
  type    = string
  default = "floor"
}

variable "source_ingest_slice_range_policy" {
  type    = string
  default = "overlap"
}

variable "source_ingest_container_image" {
  type = string
}

variable "standardize_container_image" {
  type = string
}

variable "standardize_slice_selector_mode" {
  type    = string
  default = "current"
}

variable "standardize_slice_pinned_at" {
  type    = string
  default = null
}

variable "standardize_slice_range_start_at" {
  type    = string
  default = null
}

variable "standardize_slice_range_end_at" {
  type    = string
  default = null
}

variable "standardize_slice_relative_count" {
  type    = number
  default = null
}

variable "standardize_slice_relative_direction" {
  type    = string
  default = null
}

variable "standardize_slice_relative_anchor_at" {
  type    = string
  default = null
}

variable "standardize_slice_alignment_policy" {
  type    = string
  default = "floor"
}

variable "standardize_slice_range_policy" {
  type    = string
  default = "overlap"
}

variable "standardize_output_slice_granularity" {
  type    = string
  default = "day"
}

variable "standardize_processed_base_prefix" {
  type    = string
  default = "bronze"
}

variable "standardize_processed_partition_fields_json" {
  type    = string
  default = null
}

variable "standardize_processed_path_suffix_json" {
  type    = string
  default = null
}
