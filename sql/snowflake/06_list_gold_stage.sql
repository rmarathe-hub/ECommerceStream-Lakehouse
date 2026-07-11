-- List curated gold files on the external stage (no COPY INTO).
-- Uses the verified stage: COMMERCESTREAM_DB.RAW.S3_GOLD_STAGE
-- Requires DE_PROJECT_WH (auto-resumes). Always suspend after.

USE WAREHOUSE DE_PROJECT_WH;
USE DATABASE COMMERCESTREAM_DB;

LIST @COMMERCESTREAM_DB.RAW.S3_GOLD_STAGE;
