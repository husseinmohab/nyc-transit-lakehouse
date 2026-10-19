variable "aws_region" {
  description = "AWS region. us-east-1 keeps the lakehouse next to the MTA's own publishing region, minimising ingest latency and cross-region transfer cost."
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project slug used as the prefix for every resource name."
  type        = string
  default     = "nyc-transit-lakehouse"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,30}$", var.project))
    error_message = "project must be lowercase alphanumeric with hyphens, 3-31 chars (S3 bucket name constraints)."
  }
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of dev, staging, prod."
  }
}

variable "create_network" {
  description = <<-EOT
    Create a VPC for this stack instead of being given one. The brokers have no
    public endpoint under SASL/IAM, so something has to live in the same VPC;
    set this when there is no existing VPC to put it in.
  EOT
  type        = bool
  default     = false
}

variable "vpc_id" {
  description = "Existing VPC for the MSK brokers. Ignored when create_network is true."
  type        = string
  default     = ""
}

variable "private_subnet_ids" {
  description = "Private subnets for MSK brokers. Must be in at least two AZs; three is required for the 3-broker default. Ignored when create_network is true."
  type        = list(string)
  default     = []

  validation {
    condition     = length(var.private_subnet_ids) == 0 || length(var.private_subnet_ids) >= 2
    error_message = "MSK requires subnets in at least two availability zones."
  }
}

variable "vpc_cidr_block" {
  description = "CIDR for the created VPC. Only read when create_network is true."
  type        = string
  default     = "10.0.0.0/16"
}

variable "create_test_host" {
  description = <<-EOT
    Launch one in-VPC EC2 instance to exercise the stack against real AWS. A
    test harness, not part of the production design: it holds the union of the
    producer and streaming permissions because an instance profile can carry
    only one role. Off by default.
  EOT
  type        = bool
  default     = false
}

variable "test_host_instance_type" {
  type    = string
  default = "t3.large"
}

variable "test_host_repo_ref" {
  description = "Branch, tag or commit the test host checks out."
  type        = string
  default     = "main"
}

variable "allowed_client_cidrs" {
  description = "CIDRs allowed to reach the MSK brokers. Defaults to RFC1918 within the VPC; never 0.0.0.0/0."
  type        = list(string)
  default     = ["10.0.0.0/8"]

  validation {
    condition     = !contains(var.allowed_client_cidrs, "0.0.0.0/0")
    error_message = "Refusing to expose Kafka brokers to 0.0.0.0/0."
  }
}

# MSK sizing
variable "msk_kafka_version" {
  description = <<-EOT
    MSK no longer accepts Kafka versions below 3.8 for new clusters on standard
    brokers; CreateCluster rejects 3.6.0 outright. `aws kafka
    list-kafka-versions` is the live list for an account.
  EOT
  type        = string
  default     = "3.9.x"
}

variable "msk_broker_count" {
  description = "Number of brokers. Must be a multiple of the AZ count so partitions distribute evenly."
  type        = number
  default     = 3
}

variable "msk_instance_type" {
  description = <<-EOT
    Broker instance type. kafka.m5.large sustains well above the ~2k events/sec
 this pipeline produces (roughly 0.5 MB/s at the measured ~215 bytes/record);
 it is chosen for the 3-AZ redundancy and headroom, not for raw throughput.
    See docs/benchmark.md for the measured figures behind this.
  EOT
  type        = string
  default     = "kafka.m5.large"
}

variable "msk_ebs_volume_size_gb" {
  description = <<-EOT
    Per-broker EBS size. At ~0.5 MB/s ingest and 7-day retention the topics hold
 roughly 300 GB replicated 3x; 200 GB per broker across 3 brokers leaves
 headroom for a backlog without auto-scaling kicking in mid-incident.
  EOT
  type        = number
  default     = 200
}

variable "kafka_topics" {
  description = "Topics to create, with their partition and retention settings."
  type = map(object({
    partitions         = number
    replication_factor = number
    retention_hours    = number
  }))
  default = {
    "mta.gtfs.trip_updates.v1" = {
      # Trip updates are ~95% of the volume. 12 partitions lets 12 Spark tasks
      # consume in parallel, and the key (service_date|trip_id) has high enough
      # cardinality to spread evenly across them.
      partitions         = 12
      replication_factor = 3
      retention_hours    = 168 # 7 days: long enough to replay a week from Kafka
    }
    "mta.gtfs.vehicle_positions.v1" = {
      partitions         = 6
      replication_factor = 3
      retention_hours    = 168
    }
    "mta.gtfs.alerts.v1" = {
      # Very low volume, so more partitions would only fragment ordering.
      partitions         = 3
      replication_factor = 3
      retention_hours    = 720 # 30 days: alerts are small and the history is useful
    }
  }
}

# Lakehouse storage
variable "lakehouse_bucket_name" {
  description = "S3 bucket for the Iceberg warehouse. Empty generates <project>-<environment>-lakehouse-<suffix>."
  type        = string
  default     = ""
}

variable "enable_bucket_versioning" {
  description = <<-EOT
    S3 versioning on the warehouse bucket. Default false: Iceberg already gives
 snapshot-level time travel, so object versioning duplicates that guarantee
 at full storage cost, and versioned delete markers make
 remove_orphan_files much harder to reason about.
  EOT
  type        = bool
  default     = false
}

variable "noncurrent_version_expiration_days" {
  description = "Days before noncurrent object versions expire (only meaningful with versioning on)."
  type        = number
  default     = 30
}

variable "bronze_transition_to_ia_days" {
  description = "Days before bronze Parquet transitions to S3 Infrequent Access. Bronze is the replay substrate: read rarely, kept always."
  type        = number
  default     = 90
}

variable "glue_database_names" {
  description = "Glue databases, one per medallion layer."
  type        = list(string)
  # staging is here because dbt's staging models need a database of their own
  # on the spark target. See dbt/nyc_transit/macros/generate_schema_name.sql.
  default = ["bronze", "silver", "marts", "snapshots", "staging"]
}

variable "tags" {
  description = "Extra tags merged into every resource."
  type        = map(string)
  default     = {}
}
