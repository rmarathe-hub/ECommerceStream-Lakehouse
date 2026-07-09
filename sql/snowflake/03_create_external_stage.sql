-- External stage over S3 gold/ — lists and loads curated Parquet only.
-- No COPY INTO in this phase.

USE DATABASE COMMERCESTREAM_DB;
USE SCHEMA STAGING;

CREATE OR REPLACE STAGE COMMERCESTREAM_GOLD_STAGE
  STORAGE_INTEGRATION = COMMERCESTREAM_S3_INT
  URL = 's3://&{aws_s3_bucket}/gold/'
  FILE_FORMAT = COMMERCESTREAM_PARQUET_FF
  COMMENT = 'Curated gold marts from S3 (fct_*, agg_*, dq summary)';
