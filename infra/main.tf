# ---------------------------------------------------------------------------
# Root module: wires the four child modules together.
#
# Dependency order is expressed through variable references rather than
# depends_on, so Terraform can parallelise what is independent:
# s3_lakehouse  (no dependencies)
# glue_catalog  (needs the warehouse location)
# msk           (needs the VPC/subnets)
# iam           (needs the bucket ARN, the Glue catalog id and the MSK ARN)
# ---------------------------------------------------------------------------

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
  vpc_id               = var.vpc_id
  subnet_ids           = var.private_subnet_ids
  allowed_client_cidrs = var.allowed_client_cidrs
  topics               = var.kafka_topics
  logs_bucket_arn      = module.s3_lakehouse.logs_bucket_arn
  logs_bucket_name     = module.s3_lakehouse.logs_bucket_name
  tags                 = local.common_tags
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
  tags                  = local.common_tags
}
