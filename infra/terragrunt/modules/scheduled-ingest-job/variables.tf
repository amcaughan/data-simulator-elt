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

variable "simulator_api_url_ssm_param_name" {
  type = string
}

variable "preset_id" {
  type = string
}

variable "row_count" {
  type = number
}

variable "container_image" {
  type = string
}

variable "command" {
  type    = list(string)
  default = ["python", "-c", "print('scheduled ingest placeholder')"]
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
