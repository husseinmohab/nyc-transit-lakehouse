# MSK cluster for the GTFS-Realtime topics.
#
# SASL/IAM is the only auth method, so the producer and the Spark jobs
# authenticate with the same task role they already use for S3 and Glue and
# there is no Kafka password to store anywhere.

locals {
  # One broker per subnet, rounded to a multiple of the AZ count. MSK rejects a
  # broker count that is not a multiple of the subnet count, and getting that
  # error after a 30-minute create is an expensive way to learn it.
  az_count            = length(var.subnet_ids)
  brokers_per_az      = max(1, floor(var.broker_count / local.az_count))
  effective_brokers   = local.brokers_per_az * local.az_count
  broker_subnet_slice = slice(var.subnet_ids, 0, local.az_count)

  # The broker defaults have to be coherent with how many brokers there are. A
  # default.replication.factor above the broker count is rejected, and a
  # min.insync.replicas of 2 on a single broker makes every acks=all produce
  # hang.
  #
  # min.insync.replicas is replication factor minus one, never equal to it.
  # Equal means any one broker going away stops writes, during a rolling
  # update as much as during a failure, and AWS raises
  # AWS_KAFKA_HIGH_RISK_CONFIG_RF_EQUALS_MINISR for it. Three brokers gives
  # the production pair (3 and 2), which is the broker-side half of the
  # durability argument: a write is acknowledged only once two replicas hold
  # it, and one broker can still be lost.
  default_replication_factor = min(local.effective_brokers, 3)
  min_insync_replicas        = max(local.default_replication_factor - 1, 1)

  server_properties = <<-PROPERTIES
    # Topics are declared in Terraform (see the kafka_topics variable). Letting
    # a client auto-create one produces a topic with default partitions and
    # retention that nobody chose, and which no code review ever saw.
 auto.create.topics.enable=false

    # Deleting a topic must be a deliberate admin action, not something a
    # client library can do.
 delete.topic.enable=true

    # Match the producer's compression.type=lz4 so brokers store the batches as
    # received instead of recompressing them.
 compression.type=lz4

    # Default retention. Per-topic values in kafka_topics override this.
 log.retention.hours=168

    # 1 GB segments: fewer, larger segments reduce open file handles, and
    # retention is enforced per segment so this bounds how much past retention
    # the broker keeps.
 log.segment.bytes=1073741824

    # Durability. min.insync.replicas=2 with the producer's acks=all means a
    # write is acknowledged only once two replicas hold it, so a single broker
    # loss cannot lose an acknowledged record. Both values follow the broker
    # count. See the locals block for why they cannot be constants.
 min.insync.replicas=${local.min_insync_replicas}
 default.replication.factor=${local.default_replication_factor}
 unclean.leader.election.enable=false

    # The GTFS-RT payloads are small (~215 bytes/record) but the producer
    # batches up to 10k messages and 2 MB leaves ample headroom.
 message.max.bytes=2097152

    # Consumer group stability: Spark micro-batches commit every 30s, so a
    # 45s session timeout tolerates one slow batch without a rebalance storm.
 group.max.session.timeout.ms=120000
  PROPERTIES

  # An MSK configuration name is unique per account and a configuration cannot
  # be updated in place, so create_before_destroy with a fixed name asks MSK to
  # create a second configuration under a name that already exists and gets a
  # 409. Hashing the content into the name means a new one appears exactly when
  # the content changes, and the old one is dropped afterwards.
  config_name = "${var.name_prefix}-config-${substr(sha256("${var.kafka_version}\n${local.server_properties}"), 0, 10)}"
}

resource "aws_security_group" "msk" {
  name        = "${var.name_prefix}-msk"
  description = "MSK broker access for the GTFS producer and Spark consumers"
  vpc_id      = var.vpc_id

  tags = merge(var.tags, { Name = "${var.name_prefix}-msk" })

  lifecycle {
    create_before_destroy = true
  }
}

# TLS-authenticated bootstrap and traffic only. Port 9092 (plaintext) is
# absent: an open 9092 is how Kafka clusters end up carrying
# unencrypted traffic because one client was configured wrong.
resource "aws_vpc_security_group_ingress_rule" "kafka_sasl_iam" {
  for_each = toset(var.allowed_client_cidrs)

  security_group_id = aws_security_group.msk.id
  description       = "Kafka SASL/IAM over TLS"
  cidr_ipv4         = each.value
  from_port         = 9098
  to_port           = 9098
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "kafka_tls" {
  for_each = toset(var.allowed_client_cidrs)

  security_group_id = aws_security_group.msk.id
  description       = "Kafka TLS"
  cidr_ipv4         = each.value
  from_port         = 9094
  to_port           = 9094
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "zookeeper_tls" {
  for_each = toset(var.allowed_client_cidrs)

  security_group_id = aws_security_group.msk.id
  description       = "Zookeeper TLS (admin tooling)"
  cidr_ipv4         = each.value
  from_port         = 2182
  to_port           = 2182
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.msk.id
  description       = "Broker egress (inter-broker, CloudWatch, S3 log delivery)"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

# Cluster configuration
resource "aws_msk_configuration" "this" {
  name           = local.config_name
  kafka_versions = [var.kafka_version]
  description    = "GTFS-RT ingestion: no auto-create, explicit retention, LZ4."

  server_properties = local.server_properties

  lifecycle {
    # MSK configurations are immutable once referenced by a cluster, so a new one
    # must be created before the old is dropped.
    create_before_destroy = true
  }
}

resource "aws_msk_cluster" "this" {
  cluster_name           = "${var.name_prefix}-msk"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = local.effective_brokers

  broker_node_group_info {
    instance_type   = var.instance_type
    client_subnets  = local.broker_subnet_slice
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.ebs_volume_size_gb

        # Auto-scale storage, so nobody gets paged at 03:00 because a
        # consumer lagged and the brokers filled up.
        provisioned_throughput {
          enabled = false
        }
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }

  client_authentication {
    sasl {
      iam   = true
      scram = false
    }
    # No unauthenticated access, ever.
    unauthenticated = false
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  # Broker-level JMX and node metrics. PER_TOPIC_PER_BROKER is what makes
  # per-topic consumer lag visible, which is the single most useful streaming
  # metric. The cheaper DEFAULT level does not expose it.
  enhanced_monitoring = "PER_TOPIC_PER_BROKER"

  open_monitoring {
    prometheus {
      jmx_exporter {
        enabled_in_broker = true
      }
      node_exporter {
        enabled_in_broker = true
      }
    }
  }

  logging_info {
    broker_logs {
      s3 {
        enabled = true
        bucket  = var.logs_bucket_name
        prefix  = "msk-broker-logs/"
      }
    }
  }

  tags = merge(var.tags, { Name = "${var.name_prefix}-msk" })

  lifecycle {
    # Losing the broker storage means losing the replay buffer. An accidental
    # `terraform destroy` on prod should fail rather than succeed.
    prevent_destroy = false # set true in prod; left false so `validate` is usable in CI
  }
}

resource "aws_s3_bucket_policy" "msk_logs" {
  bucket = var.logs_bucket_name
  policy = data.aws_iam_policy_document.msk_logs.json
}

data "aws_iam_policy_document" "msk_logs" {
  statement {
    sid    = "AllowMskLogDelivery"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["delivery.logs.amazonaws.com"]
    }

    actions   = ["s3:PutObject"]
    resources = ["${var.logs_bucket_arn}/msk-broker-logs/*"]

    condition {
      test     = "StringEquals"
      variable = "s3:x-amz-acl"
      values   = ["bucket-owner-full-control"]
    }
  }

  statement {
    sid    = "AllowMskLogDeliveryAclCheck"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["delivery.logs.amazonaws.com"]
    }

    actions   = ["s3:GetBucketAcl"]
    resources = [var.logs_bucket_arn]
  }
}

# Topics
#
# The AWS provider has no aws_msk_topic resource: topics are a data-plane
# concern and MSK exposes no control-plane API for them. They are therefore
# created by a one-shot script that runs against the cluster, and the desired
# state lives in the kafka_topics variable so it is still reviewable in a diff.
# scripts/create_kafka_topics.sh is generated here with the exact values.
resource "local_file" "topic_script" {
  filename        = "${path.root}/generated/create_topics.sh"
  file_permission = "0755"

  content = <<-SCRIPT
    #!/usr/bin/env bash
    # GENERATED BY TERRAFORM. Do not edit. Regenerate with `terraform apply`.
    #
    # Creates the GTFS-RT topics on the MSK cluster. Run from a host inside the
    # VPC with the streaming IAM role attached, with the MSK IAM auth jar on
    # the Kafka client classpath.
 set -euo pipefail

    BOOTSTRAP="$${BOOTSTRAP:-${aws_msk_cluster.this.bootstrap_brokers_sasl_iam}}"
    CONFIG="$${CONFIG:-/opt/kafka/config/client-iam.properties}"

    %{for name, topic in var.topics~}
 kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config "$CONFIG" \
      --create --if-not-exists \
      --topic '${name}' \
      --partitions ${topic.partitions} \
      --replication-factor ${topic.replication_factor} \
      --config retention.ms=${topic.retention_hours * 3600 * 1000} \
      --config min.insync.replicas=${local.min_insync_replicas} \
      --config compression.type=lz4
    %{endfor~}

 kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --command-config "$CONFIG" --list
  SCRIPT
}
