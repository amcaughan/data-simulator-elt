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

output "landing_bucket_name" {
  value = module.storage.landing_bucket_name
}

output "processed_bucket_name" {
  value = module.storage.processed_bucket_name
}

output "marts_bucket_name" {
  value = module.storage.marts_bucket_name
}

output "source_ingest_job_name" {
  value = module.source_ingest.job_name
}

output "source_ingest_task_definition_arn" {
  value = module.source_ingest.task_definition_arn
}

output "standardize_job_name" {
  value = module.standardize.job_name
}

output "standardize_task_definition_arn" {
  value = module.standardize.task_definition_arn
}

output "dbt_job_name" {
  value = module.dbt.job_name
}

output "dbt_task_definition_arn" {
  value = module.dbt.task_definition_arn
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
