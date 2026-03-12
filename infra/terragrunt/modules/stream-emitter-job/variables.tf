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

variable "simulator_api_url_ssm_param_name" {
  type = string
}

variable "preset_id" {
  type = string
}

variable "emission_rate_per_minute" {
  type = number
}

variable "stream_name" {
  type = string
}

variable "stream_arn" {
  type = string
}

variable "container_image" {
  type = string
}

variable "command" {
  type    = list(string)
  default = ["python", "-c", "print('stream emitter placeholder')"]
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
