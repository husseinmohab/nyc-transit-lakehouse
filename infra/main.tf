# Root module. Wires the child modules together.
#
# Dependency order comes from variable references, so Terraform can parallelise
# what is independent:
# s3_lakehouse  (no dependencies)
# glue_catalog  (needs the warehouse location)
# msk           (needs the VPC/subnets)
# iam           (needs the bucket ARN, the Glue catalog id and the MSK ARN)

# Optional, since an existing VPC can be supplied. Using count here keeps one
# `terraform apply` producing one stack.
module "vpc" {
  source = "./modules/vpc"
  count  = var.create_network ? 1 : 0

  name_prefix = local.name_prefix
  cidr_block  = var.vpc_cidr_block
  az_count    = 2
  tags        = local.common_tags
}

check "network_is_resolvable" {
  assert {
    condition     = var.create_network || (var.vpc_id != "" && length(var.private_subnet_ids) >= 2)
    error_message = "Set create_network = true, or supply vpc_id and at least two private_subnet_ids."
  }
}

module "s3_lakehouse" {
  source = "./modules/s3_lakehouse"

  bucket_name                        = local.lakehouse_bucket_name
  name_prefix                        = local.name_prefix
  enable_versioning                  = var.enable_bucket_versioning
  noncurrent_version_expiration_days = var.noncurrent_version_expiration_days
  bronze_transition_to_ia_days       = var.bronze_transition_to_ia_days
  tags                               = local.common_tags
}

module "glue_catalog" {
  source = "./modules/glue_catalog"

  name_prefix    = local.name_prefix
  database_names = var.glue_database_names
  warehouse_uri  = module.s3_lakehouse.warehouse_uri
  tags           = local.common_tags
}

module "msk" {
  source = "./modules/msk"

  name_prefix          = local.name_prefix
  kafka_version        = var.msk_kafka_version
  broker_count         = var.msk_broker_count
  instance_type        = var.msk_instance_type
  ebs_volume_size_gb   = var.msk_ebs_volume_size_gb
  vpc_id               = local.vpc_id
  subnet_ids           = local.private_subnet_ids
  allowed_client_cidrs = var.allowed_client_cidrs
  topics               = var.kafka_topics
  logs_bucket_arn      = module.s3_lakehouse.logs_bucket_arn
  logs_bucket_name     = module.s3_lakehouse.logs_bucket_name
  tags                 = local.common_tags
}

# Athena is the transform engine for dbt against the Glue catalog. Serverless,
# so this is a workgroup and a results prefix rather than a cluster.
module "athena" {
  source = "./modules/athena"

  name_prefix         = local.name_prefix
  results_bucket_name = module.s3_lakehouse.bucket_name
  tags                = local.common_tags
}

module "iam" {
  source = "./modules/iam"

  name_prefix           = local.name_prefix
  aws_region            = var.aws_region
  lakehouse_bucket_arn  = module.s3_lakehouse.bucket_arn
  lakehouse_bucket_name = module.s3_lakehouse.bucket_name
  glue_database_names   = var.glue_database_names
  msk_cluster_arn       = module.msk.cluster_arn
  msk_cluster_name      = module.msk.cluster_name
  kafka_topic_names     = keys(var.kafka_topics)
  athena_workgroup_name = module.athena.workgroup_name
  tags                  = local.common_tags
}

# Test harness. See modules/test_host for why this is not one of the three
# production roles.
module "test_host" {
  source = "./modules/test_host"
  count  = var.create_test_host ? 1 : 0

  name_prefix = local.name_prefix
  aws_region  = var.aws_region
  vpc_id      = local.vpc_id
  subnet_id   = local.public_subnet_ids[0]

  instance_type = var.test_host_instance_type
  repo_ref      = var.test_host_repo_ref

  msk_cluster_arn       = module.msk.cluster_arn
  msk_cluster_name      = module.msk.cluster_name
  msk_bootstrap_brokers = module.msk.bootstrap_brokers_sasl_iam
  kafka_topic_names     = keys(var.kafka_topics)

  lakehouse_bucket_arn  = module.s3_lakehouse.bucket_arn
  lakehouse_bucket_name = module.s3_lakehouse.bucket_name
  glue_database_names   = var.glue_database_names
  athena_workgroup_name = module.athena.workgroup_name

  tags = local.common_tags
}
