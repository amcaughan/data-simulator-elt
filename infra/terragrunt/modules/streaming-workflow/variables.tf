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

variable "storage_locations" {
  type = map(object({
    bucket_name = optional(string)
    prefix      = optional(string)
  }))

  default = {
    process = {}
    surface = {}
  }
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

variable "athena_results_bucket_name" {
  type = string
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

variable "stream_schedule_expression" {
  type    = string
  default = null
}

variable "dbt_schedule_expression" {
  type    = string
  default = null
}

variable "dbt_container_image" {
  type    = string
  default = null
}

variable "stream_emitter_container_image" {
  type    = string
  default = null
}
