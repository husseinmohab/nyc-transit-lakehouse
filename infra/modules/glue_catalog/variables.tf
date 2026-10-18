variable "name_prefix" { type = string }

variable "database_names" {
  description = "One Glue database per medallion layer."
  type        = list(string)
}

variable "warehouse_uri" {
  description = "s3:// root of the Iceberg warehouse."
  type        = string
}

variable "tags" {
  type    = map(string)
  default = {}
}
