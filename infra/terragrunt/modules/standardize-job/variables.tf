variable "environment" {
  type = string
}

variable "project_name" {
  type    = string
  default = "data-simulator-elt"
}

variable "workflow_name" {
  type = string
}

variable "aws_region" {
  type    = string
  default = "us-east-2"
}

variable "landing_bucket_name" {
  type = string
}

variable "processed_bucket_name" {
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

variable "standardize_strategy" {
  type = string
}

variable "standardize_strategy_config_json" {
  type = string
}

variable "landing_slice_granularity" {
  type    = string
  default = "day"
}

variable "output_slice_granularity" {
  type    = string
  default = "day"
}

variable "processed_base_prefix" {
  type    = string
  default = "bronze"
}

variable "processed_partition_fields_json" {
  type    = string
  default = null
}

variable "processed_path_suffix_json" {
  type    = string
  default = null
}

variable "landing_input_prefix" {
  type    = string
  default = null
}

variable "slice_selector_mode" {
  type    = string
  default = "current"
}

variable "slice_pinned_at" {
  type    = string
  default = null
}

variable "slice_range_start_at" {
  type    = string
  default = null
}

variable "slice_range_end_at" {
  type    = string
  default = null
}

variable "slice_relative_count" {
  type    = number
  default = null
}

variable "slice_relative_direction" {
  type    = string
  default = null
}

variable "slice_relative_anchor_at" {
  type    = string
  default = null
}

variable "slice_alignment_policy" {
  type    = string
  default = "floor"
}

variable "slice_range_policy" {
  type    = string
  default = "overlap"
}

variable "container_image" {
  type = string
}

variable "command" {
  type    = list(string)
  default = ["python", "-m", "standardize"]
}

variable "cpu" {
  type    = number
  default = 256
}

variable "memory" {
  type    = number
  default = 512
}

variable "log_retention_in_days" {
  type    = number
  default = 14
}
