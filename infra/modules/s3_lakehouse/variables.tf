variable "bucket_name" {
  description = "Name of the Iceberg warehouse bucket."
  type        = string
}

variable "name_prefix" {
  description = "Prefix for the companion logs bucket."
  type        = string
}

variable "enable_versioning" {
  description = "Enable S3 object versioning on the warehouse bucket."
  type        = bool
  default     = false
}

variable "noncurrent_version_expiration_days" {
  type    = number
  default = 30
}

variable "bronze_transition_to_ia_days" {
  type    = number
  default = 90
}

variable "tags" {
  type    = map(string)
  default = {}
}
