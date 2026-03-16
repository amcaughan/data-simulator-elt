output "storage_locations" {
  value = local.resolved_storage_locations
}

output "storage_location_bucket_names" {
  value = {
    for location_name, config in local.resolved_storage_locations :
    location_name => config.bucket_name
  }
}

output "storage_location_prefixes" {
  value = {
    for location_name, config in local.resolved_storage_locations :
    location_name => config.prefix
  }
}

output "storage_location_s3_roots" {
  value = {
    for location_name, config in local.resolved_storage_locations :
    location_name => config.s3_root
  }
}
