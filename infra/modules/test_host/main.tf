# ---------------------------------------------------------------------------
# A single in-VPC host used to prove the stack runs against real AWS.
#
# This module is a test harness, not part of the production design. Production
# runs the producer and the Spark job as separate workloads holding the
# separate roles in modules/iam, which is the point of splitting those roles.
# One EC2 instance can hold exactly one instance profile, so the role here is
# the union of the producer and streaming permissions. It is named and tagged
# so it is obvious in the console that it is not one of the three.
#
# Access is through SSM Session Manager. There is no SSH key to manage, no
# inbound security group rule at all, and nothing to rotate afterwards.
# ---------------------------------------------------------------------------

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

# Amazon Linux 2023 ships the SSM agent preinstalled, which is what makes the
# zero-inbound-rule approach work without a custom AMI.
data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-kernel-6.1-x86_64"]
  }
}

locals {
  account_id  = data.aws_caller_identity.current.account_id
  partition   = data.aws_partition.current.partition
  install_dir = "/opt/nyc-transit"

  msk_arn_parts    = split("/", var.msk_cluster_arn)
  msk_cluster_uuid = element(local.msk_arn_parts, length(local.msk_arn_parts) - 1)
  msk_topic_arns = [
    for topic in var.kafka_topic_names :
    "arn:${local.partition}:kafka:${var.aws_region}:${local.account_id}:topic/${var.msk_cluster_name}/${local.msk_cluster_uuid}/${topic}"
  ]
  msk_group_arn = "arn:${local.partition}:kafka:${var.aws_region}:${local.account_id}:group/${var.msk_cluster_name}/${local.msk_cluster_uuid}/*"

  glue_catalog_arn = "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:catalog"
  glue_db_prefix   = replace(var.name_prefix, "-", "_")
  glue_db_arns = [
    for db in var.glue_database_names :
    "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:database/${local.glue_db_prefix}_${db}"
  ]
  glue_table_arns = [
    for db in var.glue_database_names :
    "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:table/${local.glue_db_prefix}_${db}/*"
  ]
}

# ---------------------------------------------------------------------------
# Security group: egress only.
#
# Nothing connects to this host. Kafka is an outbound connection to the
# brokers, and the MSK security group admits it by CIDR, so there is no
# inbound rule here and no cross-referenced group.
# ---------------------------------------------------------------------------
resource "aws_security_group" "this" {
  name        = "${var.name_prefix}-test-host"
  description = "Egress only. Reaches MSK, S3, Glue, SSM and the MTA feeds."
  vpc_id      = var.vpc_id

  egress {
    description = "All outbound. Narrowing this would mean enumerating the MTA, PyPI and Maven Central address space."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, { Name = "${var.name_prefix}-test-host" })
}

# ---------------------------------------------------------------------------
# Role
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "trust" {
  statement {
    sid     = "AllowEc2"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }

    # No aws:SourceAccount condition. EC2 does not populate that key when it
    # assumes a role for an instance profile, so a condition on it rejects
    # every assume and the instance comes up with no credentials at all.
  }
}

resource "aws_iam_role" "this" {
  name               = "${var.name_prefix}-test-host"
  description        = "Test harness only: union of the producer and streaming permissions."
  assume_role_policy = data.aws_iam_policy_document.trust.json
  tags               = merge(var.tags, { Workload = "test-harness" })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.this.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "harness" {
  statement {
    sid    = "ConnectToCluster"
    effect = "Allow"
    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeCluster",
      # Cluster level, not topic level. The producer runs with
      # enable.idempotence=true, and without this the broker refuses to hand
      # out a producer id: CLUSTER_AUTHORIZATION_FAILED on the first flush,
      # which reads like an auth problem rather than a missing action.
      "kafka-cluster:WriteDataIdempotently",
    ]
    resources = [var.msk_cluster_arn]
  }

  statement {
    sid    = "ReadWriteTopics"
    effect = "Allow"
    actions = [
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:ReadData",
      "kafka-cluster:WriteData",
      "kafka-cluster:CreateTopic",
      "kafka-cluster:AlterTopic",
      "kafka-cluster:DescribeTopicDynamicConfiguration",
      "kafka-cluster:AlterTopicDynamicConfiguration",
    ]
    resources = local.msk_topic_arns
  }

  statement {
    sid    = "ConsumerGroups"
    effect = "Allow"
    actions = [
      "kafka-cluster:DescribeGroup",
      "kafka-cluster:AlterGroup",
    ]
    resources = [local.msk_group_arn]
  }

  statement {
    sid       = "ListTheWarehouseBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [var.lakehouse_bucket_arn]
  }

  statement {
    sid    = "ReadWriteTheWarehouse"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${var.lakehouse_bucket_arn}/*"]
  }

  statement {
    sid    = "RunAthenaQueries"
    effect = "Allow"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetQueryResultsStream",
      "athena:StopQueryExecution",
      "athena:GetWorkGroup",
      "athena:GetDataCatalog",
      "athena:ListDatabases",
      "athena:ListTableMetadata",
      "athena:GetTableMetadata",
    ]
    resources = [
      "arn:${local.partition}:athena:${var.aws_region}:${local.account_id}:workgroup/${var.athena_workgroup_name}",
      "arn:${local.partition}:athena:${var.aws_region}:${local.account_id}:datacatalog/AwsDataCatalog",
    ]
  }

  statement {
    sid    = "GlueCatalog"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetTableVersion",
      "glue:GetTableVersions",
      "glue:DeleteTableVersion",
      "glue:BatchDeleteTableVersion",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchCreatePartition",
      "glue:BatchGetPartition",
      "glue:CreatePartition",
      "glue:UpdatePartition",
      "glue:DeletePartition",
    ]
    resources = concat([local.glue_catalog_arn], local.glue_db_arns, local.glue_table_arns)
  }
}

resource "aws_iam_role_policy" "harness" {
  name   = "harness"
  role   = aws_iam_role.this.id
  policy = data.aws_iam_policy_document.harness.json
}

resource "aws_iam_instance_profile" "this" {
  name = "${var.name_prefix}-test-host"
  role = aws_iam_role.this.name
  tags = var.tags
}

# ---------------------------------------------------------------------------
# Instance
# ---------------------------------------------------------------------------
resource "aws_instance" "this" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.instance_type
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [aws_security_group.this.id]
  iam_instance_profile   = aws_iam_instance_profile.this.name

  # IMDSv2 only. An SSRF in anything running here would otherwise be able to
  # read the instance credentials straight off the metadata endpoint.
  metadata_options {
    http_tokens                 = "required"
    http_endpoint               = "enabled"
    http_put_response_hop_limit = 1
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.root_volume_gb
    encrypted             = true
    delete_on_termination = true
  }

  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/bootstrap.sh.tftpl", {
    install_dir       = local.install_dir
    repo_url          = var.repo_url
    repo_ref          = var.repo_ref
    aws_region        = var.aws_region
    bootstrap_brokers = var.msk_bootstrap_brokers
    bucket_name       = var.lakehouse_bucket_name
    glue_db_prefix    = local.glue_db_prefix
  })

  tags = merge(var.tags, {
    Name     = "${var.name_prefix}-test-host"
    Workload = "test-harness"
  })
}
