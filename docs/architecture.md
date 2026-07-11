# Architecture

## Current data flow

```
1M–5M e-commerce events (local CSV samples)
        ↓
Python replay producer
        ↓
Redpanda/Kafka (local Docker)
        ↓
Spark Structured Streaming (local Docker)
        ↓
Bronze / Silver / Gold Parquet (local disk)
        ↓
S3 curated gold only  ← LIVE (~55 MB for 1M demo)
        ↓
Snowflake X-Small + dbt marts  ← LIVE (Week 4–5)
        ↓
Streamlit dashboard on mart_*  ← LIVE (Week 6)
```

## What runs where

| Stage | Runtime | Storage |
|-------|---------|---------|
| Ingest + medallion transforms | Local Spark / Redpanda | `data/bronze`, `data/silver`, `data/gold` |
| Durable cloud copy | Upload IAM user | S3 `gold/` prefix only (~55 MB for 1M demo) |
| Warehouse + dbt marts | `DE_PROJECT_WH` (XSMALL) | `COMMERCESTREAM_DB.STAGING` / `MARTS` |
| Dashboard | Local Streamlit | Reads Snowflake `MARTS.mart_*` only |

## S3 cloud-lite (implemented)

- **Terraform:** `infra/aws/` — bucket, lifecycle, upload IAM user, budget alert
- **Upload:** `make upload-gold-s3` — `src/utils/upload_gold_to_s3.py`
- **Policy:** Upload user can write `gold/*` only; raw/bronze/silver never leave the laptop

Details: [cloud_lite_s3.md](cloud_lite_s3.md)

## Snowflake + dbt (implemented)

- **Guardrails:** XSMALL warehouse, auto-suspend 60s, resource monitor (3 credits/month)
- **Stage:** `COMMERCESTREAM_DB.RAW.S3_GOLD_STAGE` → S3 `gold/`
- **Load:** `make snowflake-load-gold` — curated gold Parquet only
- **dbt:** `make dbt-build` via `.venv-dbt` → `MARTS.mart_sessions`, `mart_product_performance`, `mart_conversion_funnel`
- **One-shot:** `make cloud-lite` (upload → load → dbt → suspend)

Details: [week5_load_plan.md](week5_load_plan.md), [cost_controls.md](cost_controls.md)

## Design principles

- Heavy processing runs locally (Spark).
- Only curated gold tables go to S3 and Snowflake.
- Full 285M-event replay is documented but not run by default.
- Every Snowflake session ends with `make snowflake-suspend`.

Full guardrails: [cost_controls.md](cost_controls.md)
