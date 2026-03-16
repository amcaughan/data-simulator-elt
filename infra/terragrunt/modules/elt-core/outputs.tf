output "environment" {
  value = var.environment
}

output "project_name" {
  value = var.project_name
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.this.name
}

output "ecs_cluster_arn" {
  value = aws_ecs_cluster.this.arn
}

output "glue_database_name" {
  value = aws_athena_database.this.name
}

output "athena_workgroup_name" {
  value = aws_athena_workgroup.this.name
}

output "athena_results_bucket_name" {
  value = aws_s3_bucket.athena_results.bucket
}

output "source_ingest_ecr_repository_url" {
  value = aws_ecr_repository.this["source_ingest"].repository_url
}

output "standardize_ecr_repository_url" {
  value = aws_ecr_repository.this["standardize"].repository_url
}

output "network_vpc_id" {
  value     = data.aws_ssm_parameter.network_vpc_id.value
  sensitive = true
}

output "network_private_subnet_ids" {
  value     = split(",", data.aws_ssm_parameter.network_private_subnet_ids.value)
  sensitive = true
}

output "network_security_group_id" {
  value     = data.aws_ssm_parameter.network_shared_security_group_id.value
  sensitive = true
}
