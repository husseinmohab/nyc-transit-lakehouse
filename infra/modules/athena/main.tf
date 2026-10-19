# Athena workgroup for the dbt transform layer.
#
# dbt runs the same models here that it runs on DuckDB locally, reading and
# writing the Iceberg tables through the Glue catalog, so there is no cluster to
# size between builds.
#
# This replaces running dbt through Spark. dbt-spark builds a view for its
# snapshot staging relation, an Iceberg catalog cannot hold a view, and Athena
# supports views natively.

resource "aws_athena_workgroup" "this" {
  name        = var.name_prefix
  description = "dbt transform layer over the Iceberg lakehouse"
  state       = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    # A ceiling, not a budget alarm. Athena cancels the query instead of
    # scanning the whole lake because someone forgot a partition predicate.
    bytes_scanned_cutoff_per_query = var.bytes_scanned_cutoff

    result_configuration {
      output_location = "s3://${var.results_bucket_name}/${var.results_prefix}/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  # Results are scratch and the workgroup is reproducible from this file, so
  # there is nothing here worth protecting from a destroy.
  force_destroy = true

  tags = var.tags
}
