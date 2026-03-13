variable "aws_region" {
  type    = string
  default = "us-east-2"
}

variable "repository_url" {
  type = string
}

variable "runtime_source_dir" {
  type = string
}

variable "build_context_dir" {
  type = string
}

variable "extra_hash_dirs" {
  type = list(string)
  default = []
}

variable "extra_hash_files" {
  type    = list(string)
  default = []
}
