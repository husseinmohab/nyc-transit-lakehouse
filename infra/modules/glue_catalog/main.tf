# ---------------------------------------------------------------------------
# Glue Data Catalog databases backing the Iceberg catalog.
#
# Iceberg's GlueCatalog stores table metadata pointers in Glue; the table
# schema, partitioning and snapshot history live in the Iceberg metadata files
# in S3. That split is why there are no aws_glue_catalog_table resources here:
# the tables are created by the Spark DDL in src/nyc_transit/streaming/ddl.py,
# which is the only writer that understands Iceberg's metadata layout.
# Declaring them in Terraform as well would mean two tools owning one object.
#
# No Glue Crawler either, . A crawler infers schema by sampling
# files; Iceberg already has an exact, versioned schema. Pointing a crawler at
# an Iceberg table is a known way to corrupt the catalog entry.
# ---------------------------------------------------------------------------

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
data "aws_partition" "current" {}

locals {
  glue_arn_prefix = "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}"
}

resource "aws_glue_catalog_database" "layers" {
  for_each = toset(var.database_names)

  name        = "${replace(var.name_prefix, "-", "_")}_${each.value}"
  description = "Iceberg ${each.value} layer for ${var.name_prefix}"

  location_uri = "${var.warehouse_uri}/${each.value}"

  tags = merge(var.tags, { Layer = each.value })
}

# A Lake Formation-style resource link is not created here: this stack grants
# access through IAM policies on the Glue actions (see modules/iam). If the
# account has Lake Formation enabled in its default state, the IAM-only path
# continues to work; an account with LF enforcement on would additionally need
# aws_lakeformation_permissions, which is left out rather than guessed at.

resource "aws_glue_resource_policy" "catalog_read" {
  # Restricts who may read the catalog to this account. Glue's default is
  # already account-scoped; stating it explicitly means a future cross-account
  # grant has to be a deliberate edit to this file.
  policy = data.aws_iam_policy_document.catalog_read.json
}

data "aws_iam_policy_document" "catalog_read" {
  statement {
    sid    = "AccountScopedCatalogAccess"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = [data.aws_caller_identity.current.account_id]
    }

    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
    ]

    # Glue validates these against arn:aws:glue:<region>:<account>:<thing>, so
    # a bare "*" is rejected by PutResourcePolicy even though IAM accepts it.
    resources = [
      "${local.glue_arn_prefix}:catalog",
      "${local.glue_arn_prefix}:database/*",
      "${local.glue_arn_prefix}:table/*/*",
    ]
  }
}
