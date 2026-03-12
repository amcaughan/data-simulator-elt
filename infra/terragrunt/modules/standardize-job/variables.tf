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

variable "source_adapter" {
  type = string
}

variable "source_adapter_config_json" {
  type = string
}

variable "landing_partition_granularity" {
  type    = string
  default = "day"
}

variable "output_partition_granularity" {
  type    = string
  default = "day"
}

variable "processed_output_prefix" {
  type    = string
  default = "raw"
}

variable "landing_input_prefix" {
  type    = string
  default = null
}

variable "mode" {
  type    = string
  default = "single_run"
}

variable "logical_date" {
  type    = string
  default = null
}

variable "start_at" {
  type    = string
  default = null
}

variable "end_at" {
  type    = string
  default = null
}

variable "backfill_days" {
  type    = number
  default = null
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
