-- Parquet file format for curated gold marts (Week 5 load).
-- Day 26: metadata only — no COPY INTO.

USE DATABASE COMMERCESTREAM_DB;
USE SCHEMA STAGING;

CREATE OR REPLACE FILE FORMAT COMMERCESTREAM_PARQUET_FF
  TYPE = PARQUET
  COMPRESSION = AUTO
  COMMENT = 'Parquet format for Spark gold marts uploaded to S3';
