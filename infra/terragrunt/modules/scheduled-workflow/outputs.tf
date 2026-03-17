output "workflow_name" {
  value = var.workflow_name
}

output "aws_region" {
  value = var.aws_region
}

output "ecs_cluster_arn" {
  value = var.ecs_cluster_arn
}

output "network_private_subnet_ids" {
  value     = var.network_private_subnet_ids
  sensitive = true
}

output "network_security_group_id" {
  value     = var.network_security_group_id
  sensitive = true
}

output "storage_locations" {
  value = module.storage.storage_locations
}

output "storage_location_bucket_names" {
  value = module.storage.storage_location_bucket_names
}

output "storage_location_prefixes" {
  value = module.storage.storage_location_prefixes
}

output "storage_location_s3_roots" {
  value = module.storage.storage_location_s3_roots
}

output "source_ingest_job_name" {
  value = local.source_ingest_enabled ? module.source_ingest[0].job_name : null
}

output "source_ingest_task_definition_arn" {
  value = local.source_ingest_enabled ? module.source_ingest[0].task_definition_arn : null
}

output "standardize_job_name" {
  value = local.standardize_enabled ? module.standardize[0].job_name : null
}

output "standardize_task_definition_arn" {
  value = local.standardize_enabled ? module.standardize[0].task_definition_arn : null
}

output "dbt_job_name" {
  value = local.dbt_enabled ? module.dbt[0].job_name : null
}

output "dbt_task_definition_arn" {
  value = local.dbt_enabled ? module.dbt[0].task_definition_arn : null
}

output "dbt_ecr_repository_url" {
  value = aws_ecr_repository.dbt.repository_url
}

output "source_ingest_schedule_name" {
  value = local.source_schedule_enabled ? aws_scheduler_schedule.source_ingest[0].name : null
}

output "standardize_schedule_name" {
  value = local.standardize_schedule_enabled ? aws_scheduler_schedule.standardize[0].name : null
}

output "dbt_schedule_name" {
  value = local.dbt_schedule_enabled ? aws_scheduler_schedule.dbt[0].name : null
}
