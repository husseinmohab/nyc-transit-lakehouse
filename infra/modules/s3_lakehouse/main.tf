# Iceberg warehouse bucket plus a separate bucket for MSK broker logs.
# The MSK service principal writes the broker logs, so keeping them in their own
# bucket means the warehouse policy never has to grant it anything.

resource "aws_s3_bucket" "lakehouse" {
  bucket = var.bucket_name
  tags   = merge(var.tags, { Name = var.bucket_name, Layer = "lakehouse" })
}

resource "aws_s3_bucket_ownership_controls" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    # Disables ACLs entirely, so every access decision is made by bucket policy
    # and IAM, which is auditable.
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      # SSE-S3 rather than SSE-KMS: this is public transit data, so the
      # confidentiality requirement does not justify a KMS request charge on
      # every one of the millions of small Parquet and manifest objects an
      # Iceberg warehouse touches. A dataset with real PII would use
      # sse_algorithm = "aws:kms" and a CMK here.
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Suspended"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket     = aws_s3_bucket.lakehouse.id
  depends_on = [aws_s3_bucket_versioning.lakehouse]

  # Bronze is the replay substrate: written once, read rarely, kept forever.
  # IA after 90 days roughly halves its storage cost. It is NOT
  # moved to Glacier, because the reason for bronze is that a backfill can
  # read it today without a restore request.
  rule {
    id     = "bronze-to-infrequent-access"
    status = "Enabled"

    filter {
      prefix = "warehouse/bronze/"
    }

    transition {
      days          = var.bronze_transition_to_ia_days
      storage_class = "STANDARD_IA"
    }
  }

  # Silver and marts are queried constantly so they stay in Standard. Only the
  # multipart-upload debris is cleaned up.
  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      # A failed Spark write leaves multipart parts that are billed but
      # invisible in a normal listing. 7 days is long enough that a legitimately
      # slow large upload is never aborted.
      days_after_initiation = 7
    }
  }

  # Structured Streaming checkpoints are regenerable state, not data. They are
  # expired aggressively because a 30-day-old checkpoint is useless (Kafka
  # retention is 7 days, so its offsets no longer exist).
  rule {
    id     = "expire-old-checkpoints"
    status = "Enabled"

    filter {
      prefix = "checkpoints/"
    }

    expiration {
      days = 30
    }
  }

  rule {
    id     = "expire-noncurrent-versions"
    status = var.enable_versioning ? "Enabled" : "Disabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_expiration_days
    }
  }
}

resource "aws_s3_bucket_policy" "lakehouse_tls_only" {
  bucket = aws_s3_bucket.lakehouse.id
  policy = data.aws_iam_policy_document.lakehouse_tls_only.json

  depends_on = [aws_s3_bucket_public_access_block.lakehouse]
}

data "aws_iam_policy_document" "lakehouse_tls_only" {
  # Deny any request not over TLS. IAM policies grant. Only a bucket policy
  # can deny across every principal, including future ones, which is why this
  # lives here, not in modules/iam.
  statement {
    sid    = "DenyUnencryptedTransport"
    effect = "Deny"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = ["s3:*"]

    resources = [
      aws_s3_bucket.lakehouse.arn,
      "${aws_s3_bucket.lakehouse.arn}/*",
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

# MSK broker logs
resource "aws_s3_bucket" "logs" {
  bucket = "${var.name_prefix}-msk-logs-${substr(sha256(var.bucket_name), 0, 8)}"
  tags   = merge(var.tags, { Layer = "logs" })
}

resource "aws_s3_bucket_public_access_block" "logs" {
  bucket = aws_s3_bucket.logs.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id

  rule {
    id     = "expire-broker-logs"
    status = "Enabled"

    filter {}

    expiration {
      days = 30
    }
  }
}
