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

output "lifecycle_rules" {
  description = "S3 lifecycle expiration days per prefix (gold/ has no expiration)."
  value = {
    temp_expiration_days          = var.lifecycle_temp_expiration_days
    checkpoints_expiration_days   = var.lifecycle_checkpoints_expiration_days
    bronze_sample_expiration_days = var.lifecycle_bronze_sample_expiration_days
    gold_retained                 = true
  }
}

output "budget_name" {
  description = "AWS budget name (null if budget alert is disabled or no emails configured)."
  value       = var.create_budget_alert && length(var.budget_alert_emails) > 0 ? aws_budgets_budget.lakehouse[0].name : null
}

output "budget_monthly_limit_usd" {
  description = "Configured monthly AWS budget limit in USD."
  value       = var.create_budget_alert && length(var.budget_alert_emails) > 0 ? var.budget_monthly_limit_usd : null
}

output "budget_alert_thresholds" {
  description = "Budget alert threshold percentages."
  value       = var.create_budget_alert && length(var.budget_alert_emails) > 0 ? var.budget_alert_thresholds : null
}
