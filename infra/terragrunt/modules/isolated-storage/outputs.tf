output "landing_bucket_name" {
  value = aws_s3_bucket.this["landing"].bucket
}

output "raw_bucket_name" {
  value = aws_s3_bucket.this["raw"].bucket
}

output "analytics_bucket_name" {
  value = aws_s3_bucket.this["analytics"].bucket
}
