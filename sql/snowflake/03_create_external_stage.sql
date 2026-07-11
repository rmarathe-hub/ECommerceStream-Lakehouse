-- External stage over S3 gold/ — lists and loads curated Parquet only.
-- Canonical stage used by load scripts: COMMERCESTREAM_DB.RAW.S3_GOLD_STAGE
-- (Matches the manually verified stage that LIST'd 225 gold files.)

USE DATABASE COMMERCESTREAM_DB;
USE SCHEMA RAW;

CREATE OR REPLACE STAGE S3_GOLD_STAGE
  STORAGE_INTEGRATION = COMMERCESTREAM_S3_INT
  URL = 's3://&{aws_s3_bucket}/gold/'
  COMMENT = 'Curated gold marts from S3 (fct_*, agg_*, dq summary)';
