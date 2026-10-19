output "workgroup_name" {
  value = aws_athena_workgroup.this.name
}

output "results_uri" {
  description = "What dbt-athena takes as s3_staging_dir."
  value       = "s3://${var.results_bucket_name}/${var.results_prefix}/"
}
