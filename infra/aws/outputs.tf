output "bucket_name" {
  description = "Name of the lakehouse S3 bucket."
  value       = aws_s3_bucket.lakehouse.bucket
}

output "bucket_arn" {
  description = "ARN of the lakehouse S3 bucket."
  value       = aws_s3_bucket.lakehouse.arn
}

output "bucket_region" {
  description = "AWS region where the bucket is provisioned."
  value       = var.region
}

output "gold_prefix" {
  description = "S3 prefix for curated gold Parquet uploads (only layer eligible for cloud)."
  value       = "gold/"
}

output "upload_user_name" {
  description = "IAM user name for curated gold uploads (empty if create_upload_user is false)."
  value       = var.create_upload_user ? aws_iam_user.uploader[0].name : null
}

output "upload_user_arn" {
  description = "ARN of the dedicated upload IAM user."
  value       = var.create_upload_user ? aws_iam_user.uploader[0].arn : null
}

output "upload_policy_arn" {
  description = "ARN of the least-privilege S3 upload IAM policy."
  value       = var.create_upload_user ? aws_iam_policy.upload[0].arn : null
}

output "upload_access_key_id" {
  description = "Access key ID for the upload IAM user. Copy to local .env after apply — never commit."
  value       = var.create_upload_user && var.create_upload_access_key ? aws_iam_access_key.uploader[0].id : null
}

output "upload_secret_access_key" {
  description = "Secret access key for the upload IAM user. Copy to local .env after apply — never commit."
  value       = var.create_upload_user && var.create_upload_access_key ? aws_iam_access_key.uploader[0].secret : null
  sensitive   = true
}
