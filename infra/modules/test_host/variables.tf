variable "name_prefix" { type = string }
variable "aws_region" { type = string }
variable "vpc_id" { type = string }

variable "subnet_id" {
  description = "A public subnet. The host needs egress to poll the MTA feeds and to pull jars."
  type        = string
}

variable "instance_type" {
  description = <<-EOT
    Sized for the Spark job rather than the producer. Local-mode Spark with the
    Iceberg, Kafka and MSK IAM jars wants more than the 4 GB a t3.medium gives
    once the driver and one executor are both resident.
  EOT
  type        = string
  default     = "t3.large"
}

variable "root_volume_gb" {
  description = "pyspark plus the resolved jar set is several GB, and 8 GB (the AMI default) does not fit it."
  type        = number
  default     = 30
}

variable "repo_url" {
  type    = string
  default = "https://github.com/husseinmohab/nyc-transit-lakehouse.git"
}

variable "repo_ref" {
  description = "Branch, tag or commit to check out. The host runs exactly what is published."
  type        = string
  default     = "main"
}

variable "msk_cluster_arn" { type = string }
variable "msk_cluster_name" { type = string }
variable "msk_bootstrap_brokers" { type = string }
variable "kafka_topic_names" { type = list(string) }

variable "lakehouse_bucket_arn" { type = string }
variable "lakehouse_bucket_name" { type = string }
variable "glue_database_names" { type = list(string) }

variable "tags" {
  type    = map(string)
  default = {}
}

variable "athena_workgroup_name" {
  description = "Workgroup the harness may run dbt queries in."
  type        = string
  default     = ""
}
