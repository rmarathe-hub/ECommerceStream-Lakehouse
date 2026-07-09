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

# Prefix layout (created on first upload; documented for Day 17 lifecycle rules):
#   gold/           — curated marts only (retain)
#   temp/           — short-lived uploads (delete after 1 day, Day 17)
#   checkpoints/    — streaming checkpoints if ever synced (delete after 7 days, Day 17)
#   bronze/sample/  — optional small samples (delete after 30 days, Day 17)

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "expire-temp"
    status = "Enabled"

    filter {
      prefix = "temp/"
    }

    expiration {
      days = 1
    }
  }

  rule {
    id     = "expire-checkpoints"
    status = "Enabled"

    filter {
      prefix = "checkpoints/"
    }

    expiration {
      days = 7
    }
  }

  rule {
    id     = "expire-bronze-sample"
    status = "Enabled"

    filter {
      prefix = "bronze/sample/"
    }

    expiration {
      days = 30
    }
  }

  # gold/ — no expiration rule; curated outputs retained
}
