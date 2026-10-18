# ---------------------------------------------------------------------------
# Three roles, one per workload, scoped to what that workload actually does.
#
# producer  : Kafka WRITE only. No S3. No Glue.
# streaming : Kafka READ + S3/Glue WRITE. No Kafka write.
# dbt       : S3/Glue read-write on silver+marts, S3 READ on bronze. No Kafka.
#
# Splitting them matters because of what each one cannot do. The producer holds
# the only credential exposed to the public internet (it polls api-endpoint.mta.info),
# so it must not be able to touch the lakehouse. The dbt role runs user-authored
# SQL, so it must not be able to delete bronze. Bronze is the replay substrate
# and losing it turns a transform bug into permanent data loss.
#
# Every policy is resource-scoped. There is no "*" resource on a mutating
# action anywhere in this file.
# ---------------------------------------------------------------------------

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition

  glue_catalog_arn = "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:catalog"
  glue_db_arns = [
    for db in var.glue_database_names :
    "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:database/${replace(var.name_prefix, "-", "_")}_${db}"
  ]
  glue_table_arns = [
    for db in var.glue_database_names :
    "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:table/${replace(var.name_prefix, "-", "_")}_${db}/*"
  ]

  # MSK SASL/IAM authorises at cluster, topic and group granularity. The topic
  # ARN pattern embeds the cluster's unique id, so it is derived from the
  # cluster ARN rather than reconstructed.
  msk_arn_parts     = split("/", var.msk_cluster_arn)
  msk_cluster_uuid  = element(local.msk_arn_parts, length(local.msk_arn_parts) - 1)
  msk_topic_arn_fmt = "arn:${local.partition}:kafka:${var.aws_region}:${local.account_id}:topic/${var.msk_cluster_name}/${local.msk_cluster_uuid}"
  msk_group_arn_fmt = "arn:${local.partition}:kafka:${var.aws_region}:${local.account_id}:group/${var.msk_cluster_name}/${local.msk_cluster_uuid}"

  bronze_prefix = "${var.lakehouse_bucket_arn}/warehouse/bronze/*"
}

# ---------------------------------------------------------------------------
# Trust policy
#
# ECS tasks and EC2 instances only. Deliberately NOT a wildcard principal and
# NOT an IAM user: there is no long-lived access key anywhere in this design,
# which is why src/nyc_transit/streaming/session.py never reads one.
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "workload_trust" {
  statement {
    sid     = "AllowEcsAndEc2Workloads"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type = "Service"
      identifiers = [
        "ecs-tasks.amazonaws.com",
        "ec2.amazonaws.com",
      ]
    }

    # Confused-deputy guard: without this, any ECS task in any account that
    # could somehow assume this role would be accepted.
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

# ---------------------------------------------------------------------------
# Producer: Kafka write only
# ---------------------------------------------------------------------------
resource "aws_iam_role" "producer" {
  name               = "${var.name_prefix}-producer"
  description        = "GTFS-RT poller: publishes to MSK. No lakehouse access."
  assume_role_policy = data.aws_iam_policy_document.workload_trust.json
  tags               = merge(var.tags, { Workload = "producer" })
}

data "aws_iam_policy_document" "producer_kafka_write" {
  statement {
    sid    = "ConnectToCluster"
    effect = "Allow"

    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeCluster",
    ]

    resources = [var.msk_cluster_arn]
  }

  statement {
    sid    = "WriteDeclaredTopicsOnly"
    effect = "Allow"

    actions = [
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:WriteData",
      # Required by the idempotent producer: librdkafka asks the broker for a
      # producer id, and without this the enable.idempotence=true setting in
      # config.py fails at startup rather than silently degrading.
      "kafka-cluster:WriteDataIdempotently",
    ]

    resources = concat(
      [for topic in var.kafka_topic_names : "${local.msk_topic_arn_fmt}/${topic}"],
      # WriteDataIdempotently is a cluster-level action.
      [var.msk_cluster_arn],
    )
  }

  # No CreateTopic. Topics are declared in Terraform; a producer that can
  # create them can create a misconfigured one at 03:00 and nobody will know
  # where it came from.
}

resource "aws_iam_role_policy" "producer_kafka_write" {
  name   = "kafka-write"
  role   = aws_iam_role.producer.id
  policy = data.aws_iam_policy_document.producer_kafka_write.json
}

# ---------------------------------------------------------------------------
# Streaming: Kafka read + lakehouse write
# ---------------------------------------------------------------------------
resource "aws_iam_role" "streaming" {
  name               = "${var.name_prefix}-streaming"
  description        = "Spark Structured Streaming: reads MSK, writes Iceberg bronze/silver."
  assume_role_policy = data.aws_iam_policy_document.workload_trust.json
  tags               = merge(var.tags, { Workload = "streaming" })
}

data "aws_iam_policy_document" "streaming_kafka_read" {
  statement {
    sid    = "ConnectToCluster"
    effect = "Allow"

    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeCluster",
      "kafka-cluster:DescribeClusterDynamicConfiguration",
    ]

    resources = [var.msk_cluster_arn]
  }

  statement {
    sid    = "ReadDeclaredTopicsOnly"
    effect = "Allow"

    actions = [
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:ReadData",
    ]

    resources = [for topic in var.kafka_topic_names : "${local.msk_topic_arn_fmt}/${topic}"]
  }

  statement {
    sid    = "ManageOwnConsumerGroups"
    effect = "Allow"

    actions = [
      "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeGroup",
    ]

    # Scoped to this project's group prefix, matching the kafka.group.id set in
    # nyc_transit.streaming.jobs.kafka_source_options. A broader grant would
    # let this role reset another team's consumer offsets.
    resources = ["${local.msk_group_arn_fmt}/nyc-transit-*"]
  }

  # No WriteData. The streaming job is a consumer; if it could produce, a bug
  # in a transform could write derived records back onto the raw topic.
}

data "aws_iam_policy_document" "streaming_lakehouse_write" {
  statement {
    sid    = "ListWarehouseBucket"
    effect = "Allow"

    actions = ["s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads"]

    resources = [var.lakehouse_bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["warehouse/*", "checkpoints/*", ""]
    }
  }

  statement {
    sid    = "ReadWriteWarehouseObjects"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]

    resources = [
      "${var.lakehouse_bucket_arn}/warehouse/*",
      # Checkpoints are the exactly-once mechanism; the streaming role is the
      # only role that may write them.
      "${var.lakehouse_bucket_arn}/checkpoints/*",
    ]
  }

  statement {
    sid    = "IcebergGlueMetadata"
    effect = "Allow"

    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchCreatePartition",
      "glue:BatchGetPartition",
      "glue:UpdatePartition",
    ]

    resources = concat([local.glue_catalog_arn], local.glue_db_arns, local.glue_table_arns)
  }

  # No glue:DeleteTable and no glue:DeleteDatabase. Dropping an Iceberg table's
  # catalog entry orphans its entire metadata tree; that should require a human
  # with elevated credentials, not a Spark job.
}

resource "aws_iam_role_policy" "streaming_kafka_read" {
  name   = "kafka-read"
  role   = aws_iam_role.streaming.id
  policy = data.aws_iam_policy_document.streaming_kafka_read.json
}

resource "aws_iam_role_policy" "streaming_lakehouse_write" {
  name   = "lakehouse-write"
  role   = aws_iam_role.streaming.id
  policy = data.aws_iam_policy_document.streaming_lakehouse_write.json
}

# ---------------------------------------------------------------------------
# dbt / Airflow transformation role
# ---------------------------------------------------------------------------
resource "aws_iam_role" "dbt" {
  name               = "${var.name_prefix}-dbt"
  description        = "dbt and Airflow transformations: silver/marts read-write, bronze read-only."
  assume_role_policy = data.aws_iam_policy_document.workload_trust.json
  tags               = merge(var.tags, { Workload = "dbt" })
}

data "aws_iam_policy_document" "dbt_lakehouse" {
  statement {
    sid       = "ListWarehouseBucket"
    effect    = "Allow"
    actions   = ["s3:GetBucketLocation", "s3:ListBucket"]
    resources = [var.lakehouse_bucket_arn]
  }

  statement {
    sid    = "ReadEverythingInTheWarehouse"
    effect = "Allow"

    actions   = ["s3:GetObject"]
    resources = ["${var.lakehouse_bucket_arn}/warehouse/*"]
  }

  statement {
    sid    = "WriteSilverAndMartsOnly"
    effect = "Allow"

    actions = [
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]

    resources = [
      "${var.lakehouse_bucket_arn}/warehouse/silver/*",
      "${var.lakehouse_bucket_arn}/warehouse/marts/*",
      "${var.lakehouse_bucket_arn}/warehouse/snapshots/*",
    ]
  }

  statement {
    sid       = "ExplicitlyDenyBronzeMutation"
    effect    = "Deny"
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = [local.bronze_prefix]
  }

  statement {
    sid    = "GlueForDbtModels"
    effect = "Allow"

    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchCreatePartition",
      "glue:BatchDeletePartition",
      "glue:UpdatePartition",
    ]

    # DeleteTable is granted here but only on the silver/marts/snapshots
    # databases: dbt drops and recreates a table on a full refresh.
    resources = concat(
      [local.glue_catalog_arn],
      [for arn in local.glue_db_arns : arn if !endswith(arn, "_bronze")],
      [for arn in local.glue_table_arns : arn if !strcontains(arn, "_bronze/")],
    )
  }

  statement {
    sid    = "ReadBronzeCatalog"
    effect = "Allow"

    actions = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:GetPartitions"]

    resources = concat(
      [local.glue_catalog_arn],
      [for arn in local.glue_db_arns : arn if endswith(arn, "_bronze")],
      [for arn in local.glue_table_arns : arn if strcontains(arn, "_bronze/")],
    )
  }
}

resource "aws_iam_role_policy" "dbt_lakehouse" {
  name   = "lakehouse-transform"
  role   = aws_iam_role.dbt.id
  policy = data.aws_iam_policy_document.dbt_lakehouse.json
}

# ---------------------------------------------------------------------------
# Shared: CloudWatch Logs for all three workloads
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "cloudwatch_logs" {
  statement {
    sid    = "WriteOwnLogStreams"
    effect = "Allow"

    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams",
    ]

    resources = [
      "arn:${local.partition}:logs:${var.aws_region}:${local.account_id}:log-group:/${var.name_prefix}/*",
    ]
  }
}

resource "aws_iam_policy" "cloudwatch_logs" {
  name        = "${var.name_prefix}-cloudwatch-logs"
  description = "Write to this project's log groups only."
  policy      = data.aws_iam_policy_document.cloudwatch_logs.json
  tags        = var.tags
}

resource "aws_iam_role_policy_attachment" "logs" {
  for_each = {
    producer  = aws_iam_role.producer.name
    streaming = aws_iam_role.streaming.name
    dbt       = aws_iam_role.dbt.name
  }

  role       = each.value
  policy_arn = aws_iam_policy.cloudwatch_logs.arn
}
