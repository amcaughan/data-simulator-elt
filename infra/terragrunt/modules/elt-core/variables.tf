variable "environment" {
  type = string
}

variable "project_name" {
  type    = string
  default = "data-simulator-elt"
}

variable "network_vpc_id_ssm_param_name" {
  type = string
}

variable "network_private_subnet_ids_ssm_param_name" {
  type = string
}

variable "network_shared_security_group_ssm_param_name" {
  type = string
}

variable "publish_ssm_parameters" {
  type    = bool
  default = true
}

variable "force_destroy_stateful_resources" {
  type    = bool
  default = true
}

variable "ssm_prefix" {
  type = string
}

variable "publish_runtime_images" {
  type    = bool
  default = true
}

variable "jobs_build_context_dir" {
  type = string
}

variable "jobs_requirements_file" {
  type = string
}

variable "common_source_dir" {
  type = string
}

variable "source_ingest_source_dir" {
  type = string
}

variable "source_ingest_dockerfile_path" {
  type = string
}

variable "standardize_source_dir" {
  type = string
}

variable "standardize_dockerfile_path" {
  type = string
}

variable "dbt_source_dir" {
  type = string
}

variable "dbt_requirements_file" {
  type = string
}

variable "dbt_dockerfile_path" {
  type = string
}
