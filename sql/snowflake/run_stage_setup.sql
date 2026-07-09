-- Day 26: S3 storage integration + external stage (no data load).
-- Run from repo root: make snowflake-stage-setup
-- Requires SNOWFLAKE_S3_STORAGE_AWS_ROLE_ARN and AWS_S3_BUCKET in .env

!set variable_substitution=true

!source sql/snowflake/01_create_file_format.sql
!source sql/snowflake/02_create_storage_integration.sql
!source sql/snowflake/03_create_external_stage.sql
!source sql/snowflake/04_verify_stage_setup.sql
