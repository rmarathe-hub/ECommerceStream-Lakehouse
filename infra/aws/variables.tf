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
