terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source = "hashicorp/aws"
      # Pinned to a minor line, not "~> 5.0": a provider minor bump has changed
      # MSK and Glue resource defaults before, and an infra apply that drifts
      # because CI picked up a newer provider is not a reproducible apply.
      version = "~> 5.80"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.5"
    }
  }

  # Remote state is intentionally commented out rather than omitted: this repo
  # is published, and a committed bucket name is a free reconnaissance gift.
  # Supply it with `terraform init -backend-config=backend.hcl`.
  #
  # backend "s3" {
  # bucket       = "my-tfstate-bucket"
  # key          = "nyc-transit-lakehouse/terraform.tfstate"
  # region       = "us-east-1"
  # encrypt      = true
  # use_lockfile = true
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.common_tags
  }
}
