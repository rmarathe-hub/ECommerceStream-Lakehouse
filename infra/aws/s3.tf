resource "aws_s3_bucket" "lakehouse" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_ownership_controls" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
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
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# S3 lifecycle — cost control for non-gold prefixes (gold/ is retained indefinitely).
# See infra/aws/README.md for prefix layout and upload policy.

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "expire-temp"
    status = "Enabled"

    filter {
      prefix = "temp/"
    }

    expiration {
      days = var.lifecycle_temp_expiration_days
    }
  }

  rule {
    id     = "expire-checkpoints"
    status = "Enabled"

    filter {
      prefix = "checkpoints/"
    }

    expiration {
      days = var.lifecycle_checkpoints_expiration_days
    }
  }

  rule {
    id     = "expire-bronze-sample"
    status = "Enabled"

    filter {
      prefix = "bronze/sample/"
    }

    expiration {
      days = var.lifecycle_bronze_sample_expiration_days
    }
  }

  # gold/ — intentionally no expiration rule; curated outputs retained
}
