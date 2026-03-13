output "landing_bucket_name" {
  value = aws_s3_bucket.this["landing"].bucket
}

output "processed_bucket_name" {
  value = aws_s3_bucket.this["processed"].bucket
}

output "marts_bucket_name" {
  value = aws_s3_bucket.this["marts"].bucket
}
