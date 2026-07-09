# Snowflake load SQL (Week 4–5)

SQL for S3 stage setup and (Week 5) gold table loads. **No raw, bronze, or silver in Snowflake.**

## Day 26 — Stage setup (no load)

| Script | Purpose |
|--------|---------|
| `01_create_file_format.sql` | Parquet file format |
| `02_create_storage_integration.sql` | S3 integration (`gold/` only) |
| `03_create_external_stage.sql` | External stage over `s3://{bucket}/gold/` |
| `04_verify_stage_setup.sql` | `SHOW` / `DESC` verification |
| `run_stage_setup.sql` | Run all of the above in order |

```bash
# One-time AWS IAM role — see infra/snowflake/README.md
# Then set SNOWFLAKE_S3_STORAGE_AWS_ROLE_ARN in .env
make snowflake-stage-setup
make snowflake-check-stage
make snowflake-suspend
```

## Week 5 — Load (not yet)

`05_load_gold_tables.sql` (planned Day 29) will `COPY INTO` staging tables from `@COMMERCESTREAM_GOLD_STAGE`. Every load session ends with `make snowflake-suspend`.

## Objects

| Object | Name |
|--------|------|
| File format | `COMMERCESTREAM_DB.STAGING.COMMERCESTREAM_PARQUET_FF` |
| Storage integration | `COMMERCESTREAM_S3_INT` |
| External stage | `COMMERCESTREAM_DB.STAGING.COMMERCESTREAM_GOLD_STAGE` |
