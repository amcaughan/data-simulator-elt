output "workflow_name" {
  value = var.workflow_name
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

output "stream_emitter_job_name" {
  value = local.stream_emitter_enabled ? module.stream_emitter[0].job_name : null
}

output "stream_emitter_task_definition_arn" {
  value = local.stream_emitter_enabled ? module.stream_emitter[0].task_definition_arn : null
}

output "dbt_job_name" {
  value = local.dbt_enabled ? module.dbt[0].job_name : null
}

output "dbt_task_definition_arn" {
  value = local.dbt_enabled ? module.dbt[0].task_definition_arn : null
}

output "stream_emitter_ecr_repository_url" {
  value = aws_ecr_repository.stream_emitter.repository_url
}

output "dbt_ecr_repository_url" {
  value = aws_ecr_repository.dbt.repository_url
}

output "kinesis_stream_name" {
  value = aws_kinesis_stream.this.name
}

output "firehose_delivery_stream_name" {
  value = aws_kinesis_firehose_delivery_stream.this.name
}

output "stream_schedule_name" {
  value = local.stream_schedule_enabled ? aws_scheduler_schedule.stream_emitter[0].name : null
}

output "dbt_schedule_name" {
  value = local.dbt_schedule_enabled ? aws_scheduler_schedule.dbt[0].name : null
}
