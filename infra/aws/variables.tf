variable "region" {
  description = "AWS region for the lakehouse S3 bucket."
  type        = string
  default     = "us-east-1"
}

variable "bucket_name" {
  description = "Globally unique S3 bucket name for curated lakehouse outputs."
  type        = string
}

variable "project_name" {
  description = "Project name used for tagging and documentation."
  type        = string
  default     = "ECommerceStream-Lakehouse"
}

variable "environment" {
  description = "Deployment environment label (e.g. dev, prod)."
  type        = string
  default     = "dev"
}

variable "create_upload_user" {
  description = "Create a dedicated IAM user with least-privilege S3 upload policy."
  type        = bool
  default     = true
}

variable "upload_user_name" {
  description = "IAM user name for curated gold S3 uploads."
  type        = string
  default     = "commercestream-lakehouse-uploader"
}

variable "create_upload_access_key" {
  description = "Create an access key for the upload IAM user (store in local .env only)."
  type        = bool
  default     = true
}

variable "lifecycle_temp_expiration_days" {
  description = "Days before objects under temp/ are expired."
  type        = number
  default     = 1

  validation {
    condition     = var.lifecycle_temp_expiration_days >= 1
    error_message = "lifecycle_temp_expiration_days must be at least 1."
  }
}

variable "lifecycle_checkpoints_expiration_days" {
  description = "Days before objects under checkpoints/ are expired."
  type        = number
  default     = 7

  validation {
    condition     = var.lifecycle_checkpoints_expiration_days >= 1
    error_message = "lifecycle_checkpoints_expiration_days must be at least 1."
  }
}

variable "lifecycle_bronze_sample_expiration_days" {
  description = "Days before objects under bronze/sample/ are expired."
  type        = number
  default     = 30

  validation {
    condition     = var.lifecycle_bronze_sample_expiration_days >= 1
    error_message = "lifecycle_bronze_sample_expiration_days must be at least 1."
  }
}

variable "create_budget_alert" {
  description = "Create a monthly AWS cost budget with email notifications."
  type        = bool
  default     = true
}

variable "budget_monthly_limit_usd" {
  description = "Monthly AWS spend limit (USD) for budget alerts."
  type        = number
  default     = 5

  validation {
    condition     = var.budget_monthly_limit_usd > 0
    error_message = "budget_monthly_limit_usd must be greater than 0."
  }
}

variable "budget_alert_emails" {
  description = "Email addresses to notify when budget thresholds are exceeded."
  type        = list(string)
  default     = []

  validation {
    condition = alltrue([
      for email in var.budget_alert_emails : can(regex("^[^@]+@[^@]+\\.[^@]+$", email))
    ])
    error_message = "Each budget_alert_emails entry must be a valid email address."
  }
}

variable "budget_alert_thresholds" {
  description = "Budget alert thresholds as percentages of monthly limit (e.g. 50, 80, 100)."
  type        = list(number)
  default     = [50, 80, 100]

  validation {
    condition     = length(var.budget_alert_thresholds) > 0 && alltrue([for t in var.budget_alert_thresholds : t > 0 && t <= 200])
    error_message = "budget_alert_thresholds must be between 1 and 200 percent."
  }
}
