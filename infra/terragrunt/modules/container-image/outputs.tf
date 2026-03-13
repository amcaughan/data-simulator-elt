output "image_tag" {
  value = local.image_tag
}

output "image_uri" {
  value = "${var.repository_url}:${local.image_tag}"
}
