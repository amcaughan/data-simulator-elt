locals {
  dockerfile_path = "${var.runtime_source_dir}/Dockerfile"
  hashed_dir_files = flatten([
    for dir_path in concat([var.runtime_source_dir], var.extra_hash_dirs) : [
      for relative_path in fileset(dir_path, "**") : "${dir_path}/${relative_path}"
      if !endswith(relative_path, "/")
    ]
  ])
  all_hash_inputs = sort(distinct(concat(local.hashed_dir_files, var.extra_hash_files)))
  source_hash = sha256(join(",", [
    for file_path in local.all_hash_inputs : "${file_path}:${filesha256(file_path)}"
  ]))
  image_tag = "sha-${substr(local.source_hash, 0, 12)}"
}

resource "terraform_data" "build_and_push" {
  triggers_replace = {
    repository_url    = var.repository_url
    dockerfile_path   = local.dockerfile_path
    build_context_dir = var.build_context_dir
    source_hash       = local.source_hash
  }

  provisioner "local-exec" {
    command = "${path.module}/build_and_push_image.sh ${var.aws_region} ${var.repository_url} ${local.image_tag} ${local.dockerfile_path} ${var.build_context_dir}"
  }
}
