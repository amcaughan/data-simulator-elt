output "workflow_name" {
  value = var.workflow_name
}

output "landing_bucket_name" {
  value = module.storage.landing_bucket_name
}

output "raw_bucket_name" {
  value = module.storage.raw_bucket_name
}

output "analytics_bucket_name" {
  value = module.storage.analytics_bucket_name
}

output "streaming_job_name" {
  value = module.streaming.job_name
}

output "dbt_job_name" {
  value = module.dbt.job_name
}

output "kinesis_stream_name" {
  value = aws_kinesis_stream.this.name
}

output "firehose_delivery_stream_name" {
  value = aws_kinesis_firehose_delivery_stream.this.name
}

output "stream_schedule_name" {
  value = aws_scheduler_schedule.streaming.name
}

output "dbt_schedule_name" {
  value = var.dbt_schedule_expression == null ? null : aws_scheduler_schedule.dbt[0].name
}
