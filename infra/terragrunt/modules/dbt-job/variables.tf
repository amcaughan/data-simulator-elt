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

variable "process_bucket_name" {
  type = string
}

variable "surface_bucket_name" {
  type = string
}

variable "process_s3_root" {
  type = string
}

variable "surface_s3_root" {
  type = string
}

variable "glue_database_name" {
  type = string
}

variable "athena_workgroup_name" {
  type = string
}

variable "athena_results_bucket_name" {
  type = string
}

variable "container_image" {
  type = string
}

variable "command" {
  type    = list(string)
  default = null
}

variable "cpu" {
  type    = number
  default = 512
}

variable "memory" {
  type    = number
  default = 1024
}

variable "log_retention_in_days" {
  type    = number
  default = 14
}
