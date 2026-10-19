variable "name_prefix" { type = string }
variable "aws_region" { type = string }
variable "lakehouse_bucket_arn" { type = string }
variable "lakehouse_bucket_name" { type = string }
variable "glue_database_names" { type = list(string) }
variable "msk_cluster_arn" { type = string }
variable "msk_cluster_name" { type = string }
variable "kafka_topic_names" { type = list(string) }

variable "tags" {
  type    = map(string)
  default = {}
}

variable "athena_workgroup_name" {
  description = "Workgroup the dbt role may run queries in."
  type        = string
  default     = ""
}

variable "athena_results_prefix" {
  description = "Prefix in the lakehouse bucket holding Athena query results."
  type        = string
  default     = "athena-results"
}
