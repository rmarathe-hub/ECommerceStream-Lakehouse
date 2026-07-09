# Dedicated least-privilege IAM user for curated gold uploads (Option A).
# Scoped to gold/, temp/, and checkpoints/ only — no raw/bronze/silver access.

data "aws_iam_policy_document" "upload" {
  count = var.create_upload_user ? 1 : 0

  statement {
    sid    = "ListAllowedPrefixes"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.lakehouse.arn,
    ]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        "gold/",
        "gold/*",
        "temp/",
        "temp/*",
        "checkpoints/",
        "checkpoints/*",
      ]
    }
  }

  statement {
    sid    = "ReadWriteGold"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
    ]
    resources = [
      "${aws_s3_bucket.lakehouse.arn}/gold/*",
    ]
  }

  statement {
    sid    = "WriteTempAndCheckpoints"
    effect = "Allow"
    actions = [
      "s3:PutObject",
    ]
    resources = [
      "${aws_s3_bucket.lakehouse.arn}/temp/*",
      "${aws_s3_bucket.lakehouse.arn}/checkpoints/*",
    ]
  }

  statement {
    sid    = "DeleteTempAndCheckpointsOnly"
    effect = "Allow"
    actions = [
      "s3:DeleteObject",
    ]
    resources = [
      "${aws_s3_bucket.lakehouse.arn}/temp/*",
      "${aws_s3_bucket.lakehouse.arn}/checkpoints/*",
    ]
  }
}

resource "aws_iam_policy" "upload" {
  count = var.create_upload_user ? 1 : 0

  name        = "${var.project_name}-upload-${var.environment}"
  description = "Least-privilege S3 upload for curated gold outputs only (${var.bucket_name})"
  policy      = data.aws_iam_policy_document.upload[0].json

  tags = merge(local.common_tags, {
    Name = "${var.project_name}-upload-policy"
  })
}

resource "aws_iam_user" "uploader" {
  count = var.create_upload_user ? 1 : 0

  name = var.upload_user_name
  path = "/lakehouse/"

  tags = merge(local.common_tags, {
    Name = var.upload_user_name
  })
}

resource "aws_iam_user_policy_attachment" "upload" {
  count = var.create_upload_user ? 1 : 0

  user       = aws_iam_user.uploader[0].name
  policy_arn = aws_iam_policy.upload[0].arn
}

resource "aws_iam_access_key" "uploader" {
  count = var.create_upload_user && var.create_upload_access_key ? 1 : 0

  user = aws_iam_user.uploader[0].name
}
