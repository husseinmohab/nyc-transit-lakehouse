output "bucket_name" {
  value = aws_s3_bucket.lakehouse.id
}

output "bucket_arn" {
  value = aws_s3_bucket.lakehouse.arn
}

output "warehouse_uri" {
  description = "s3:// URI for LAKEHOUSE_WAREHOUSE."
  value       = "s3://${aws_s3_bucket.lakehouse.id}/warehouse"
}

output "checkpoint_uri" {
  description = "s3:// URI for LAKEHOUSE_CHECKPOINT_ROOT."
  value       = "s3://${aws_s3_bucket.lakehouse.id}/checkpoints"
}

output "logs_bucket_name" {
  value = aws_s3_bucket.logs.id
}

output "logs_bucket_arn" {
  value = aws_s3_bucket.logs.arn
}
