variable "aws_region" {
  type    = string
  default = "us-east-2"
}

variable "repository_url" {
  type = string
}

variable "dockerfile_path" {
  type = string
}

variable "build_context_dir" {
  type = string
}

variable "hash_dirs" {
  type = list(string)
}

variable "hash_files" {
  type    = list(string)
  default = []
}
