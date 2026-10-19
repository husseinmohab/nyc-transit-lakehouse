variable "name_prefix" { type = string }

variable "results_bucket_name" {
  description = "Bucket for Athena query results. The lakehouse bucket under its own prefix."
  type        = string
}

variable "results_prefix" {
  description = "Prefix inside that bucket. Lifecycle-expired, these are scratch."
  type        = string
  default     = "athena-results"
}

variable "bytes_scanned_cutoff" {
  description = <<-EOT
    Per query scan ceiling in bytes. Athena bills per byte scanned, so a
    runaway query is a bill rather than a slow dashboard. 1 GB is far above
    anything these marts need and well below anything expensive.
  EOT
  type        = number
  default     = 1073741824
}

variable "tags" {
  type    = map(string)
  default = {}
}
