-- S3 storage integration for gold/ prefix only.
-- Prerequisites: IAM role + trust policy — see infra/snowflake/README.md
-- Variables (Makefile passes -D): aws_role_arn, aws_s3_bucket

CREATE OR REPLACE STORAGE INTEGRATION COMMERCESTREAM_S3_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '&{aws_role_arn}'
  STORAGE_ALLOWED_LOCATIONS = ('s3://&{aws_s3_bucket}/gold/')
  COMMENT = 'Least-privilege S3 access for curated gold marts only';
