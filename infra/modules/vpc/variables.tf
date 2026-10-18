variable "name_prefix" { type = string }

variable "cidr_block" {
  description = "VPC CIDR. Must cover allowed_client_cidrs or the brokers will refuse the producer."
  type        = string
  default     = "10.0.0.0/16"
}

variable "az_count" {
  description = <<-EOT
    Availability zones to spread across. MSK requires at least two, and
    broker_count must be a multiple of this.
  EOT
  type        = number
  default     = 2

  validation {
    condition     = var.az_count >= 2 && var.az_count <= 3
    error_message = "az_count must be 2 or 3; MSK needs at least two and this stack never needs more than three."
  }
}

variable "tags" {
  type    = map(string)
  default = {}
}
