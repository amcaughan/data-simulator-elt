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

variable "ssm_prefix" {
  type = string
}
