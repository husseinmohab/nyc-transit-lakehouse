locals {
  name_prefix = "${var.project}-${var.environment}"

  common_tags = merge(
    {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
      Repository  = "github.com/husseinmohab/nyc-transit-lakehouse"
      # Nothing in this stack is free. Tagging the cost centre at creation is
      # the only way the bill is attributable later.
      CostCenter = "data-platform"
    },
    var.tags
  )

  lakehouse_bucket_name = var.lakehouse_bucket_name != "" ? var.lakehouse_bucket_name : "${local.name_prefix}-lakehouse-${random_id.bucket_suffix.hex}"
}

# S3 bucket names are globally unique, so a fixed name makes the stack
# un-deployable twice (and squattable). The suffix is stored in state, so it is
# stable across applies.
resource "random_id" "bucket_suffix" {
  byte_length = 4
}
