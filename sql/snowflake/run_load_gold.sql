-- Day 29: Load curated gold from S3 stage into STAGING (no raw/bronze/silver).
-- Run from repo root: make snowflake-load-gold
-- Ends with warehouse suspend (via Makefile).

!source sql/snowflake/05_load_gold_tables.sql
!source sql/snowflake/07_verify_gold_load.sql
