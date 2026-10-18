variable "name_prefix" { type = string }
variable "kafka_version" { type = string }
variable "broker_count" { type = number }
variable "instance_type" { type = string }
variable "ebs_volume_size_gb" { type = number }
variable "vpc_id" { type = string }
variable "subnet_ids" { type = list(string) }
variable "allowed_client_cidrs" { type = list(string) }
variable "logs_bucket_arn" { type = string }
variable "logs_bucket_name" { type = string }

variable "topics" {
  type = map(object({
    partitions         = number
    replication_factor = number
    retention_hours    = number
  }))
}

variable "tags" {
  type    = map(string)
  default = {}
}
