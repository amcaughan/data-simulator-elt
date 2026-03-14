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
  # This defaults to true in this demo repo for teardown/setup ergonomics.
  # In a real production environment, I would normally default this to false
  # and require explicit cleanup policy decisions instead of force-destroying.
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

variable "shared_containers_build_context_dir" {
  type = string
}

variable "shared_common_container_source_dir" {
  type = string
}

variable "source_ingest_container_source_dir" {
  type = string
}

variable "standardize_container_source_dir" {
  type = string
}
