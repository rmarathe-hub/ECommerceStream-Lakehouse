# Snowflake load SQL (Week 4–5)

SQL for S3 stage setup and curated gold table loads. **No raw, bronze, or silver in Snowflake.**

## Stage setup (Day 26)

| Script | Purpose |
|--------|---------|
| `01_create_file_format.sql` | Parquet file format |
| `02_create_storage_integration.sql` | S3 integration (`gold/` only) |
| `03_create_external_stage.sql` | External stage `RAW.S3_GOLD_STAGE` over `s3://{bucket}/gold/` |
| `04_verify_stage_setup.sql` | `SHOW` / `DESC` verification |
| `06_list_gold_stage.sql` | `LIST @COMMERCESTREAM_GOLD_STAGE` |
| `run_stage_setup.sql` | Run stage setup in order |

```bash
make snowflake-stage-setup
make snowflake-check-stage
make snowflake-stage-list   # expect ~225 gold files
make snowflake-suspend
```

## Gold load (Day 29)

| Script | Purpose |
|--------|---------|
| `05_load_gold_tables.sql` | `CREATE TABLE` + `COPY INTO` for 5 gold marts |
| `07_verify_gold_load.sql` | Row counts |
| `run_load_gold.sql` | Load + verify |

Hive partition columns (`session_date`, `purchase_date`) are parsed from `METADATA$FILENAME`.

```bash
make snowflake-load-gold    # COPY INTO + verify + suspend
make snowflake-verify-load  # re-check counts + suspend
make dbt-build              # .venv-dbt only + suspend
make cloud-lite             # upload → load → dbt → suspend
```

## Objects

| Object | Name |
|--------|------|
| File format | `COMMERCESTREAM_DB.STAGING.COMMERCESTREAM_PARQUET_FF` |
| Storage integration | `COMMERCESTREAM_S3_INT` |
| External stage | `COMMERCESTREAM_DB.RAW.S3_GOLD_STAGE` |
| Staging tables | `fct_sessions`, `fct_purchases`, `agg_product_performance`, `agg_conversion_funnel`, `fct_cart_abandonment` |
