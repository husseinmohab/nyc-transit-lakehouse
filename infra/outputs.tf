output "lakehouse_bucket" {
  description = "S3 bucket holding the Iceberg warehouse."
  value       = module.s3_lakehouse.bucket_name
}

output "warehouse_uri" {
  description = "Value for LAKEHOUSE_WAREHOUSE."
  value       = module.s3_lakehouse.warehouse_uri
}

output "glue_databases" {
  description = "Glue databases created, one per medallion layer."
  value       = module.glue_catalog.database_names
}

output "msk_cluster_arn" {
  description = "ARN of the MSK cluster."
  value       = module.msk.cluster_arn
}

output "msk_bootstrap_brokers_sasl_iam" {
  description = "Value for KAFKA_BOOTSTRAP_SERVERS (SASL/IAM, TLS)."
  value       = module.msk.bootstrap_brokers_sasl_iam
  # Not a credential, but it is an internal network endpoint and there is no
  # reason to print it into CI logs on every plan.
  sensitive = true
}

output "producer_role_arn" {
  description = "IAM role for the GTFS-RT producer service (Kafka write only)."
  value       = module.iam.producer_role_arn
}

output "streaming_role_arn" {
  description = "IAM role for the Spark streaming jobs (Kafka read, S3 + Glue write)."
  value       = module.iam.streaming_role_arn
}

output "dbt_role_arn" {
  description = "IAM role for dbt / Airflow transformation runs."
  value       = module.iam.dbt_role_arn
}

output "env_file_preview" {
  description = "Paste-able env settings for the producer and streaming jobs."
  value       = <<-EOT
    KAFKA_BOOTSTRAP_SERVERS=<terraform output -raw msk_bootstrap_brokers_sasl_iam>
    KAFKA_SECURITY_PROTOCOL=SASL_SSL
    KAFKA_SASL_MECHANISM=AWS_MSK_IAM
    LAKEHOUSE_CATALOG_TYPE=glue
    LAKEHOUSE_WAREHOUSE=${module.s3_lakehouse.warehouse_uri}
    LAKEHOUSE_CHECKPOINT_ROOT=${module.s3_lakehouse.checkpoint_uri}
  EOT
}
