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

variable "simulator_api_url_ssm_param_name" {
  type = string
}

variable "source_adapter" {
  type    = string
  default = "simulator_api"
}

variable "preset_id" {
  type = string
}

variable "row_count" {
  type = number
}

variable "partition_granularity" {
  type    = string
  default = "day"
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

variable "seed_strategy" {
  type    = string
  default = "derived"
}

variable "fixed_seed" {
  type    = number
  default = null
}

variable "request_overrides_json" {
  type    = string
  default = "{}"
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
