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

variable "simulator_api_url_ssm_param_name" {
  type = string
}

variable "ingest_schedule_expression" {
  type = string
}

variable "dbt_schedule_expression" {
  type    = string
  default = null
}

variable "preset_id" {
  type = string
}

variable "row_count" {
  type = number
}

variable "ingest_container_image" {
  type = string
}

variable "dbt_container_image" {
  type = string
}
