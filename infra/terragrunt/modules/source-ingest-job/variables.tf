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

variable "source_base_url_ssm_param_name" {
  type    = string
  default = null
}

variable "source_adapter" {
  type    = string
  default = "simulator_api"
}

variable "slice_granularity" {
  type    = string
  default = "day"
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

variable "source_adapter_config_json" {
  type = string
}

variable "container_image" {
  type = string
}

variable "command" {
  type    = list(string)
  default = ["python", "-m", "source_ingest"]
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
