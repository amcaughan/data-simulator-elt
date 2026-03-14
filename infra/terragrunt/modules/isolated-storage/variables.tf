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

variable "landing_bucket_name" {
  type    = string
  default = null
}

variable "processed_bucket_name" {
  type    = string
  default = null
}

variable "marts_bucket_name" {
  type    = string
  default = null
}

variable "force_destroy_buckets" {
  # This defaults to true in this demo repo for teardown/setup ergonomics.
  # In a real production environment, I would normally default this to false
  # and require explicit cleanup policy decisions instead of force-destroying.
  type    = bool
  default = true
}
