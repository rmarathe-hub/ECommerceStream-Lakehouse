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
S3 curated gold only  ← LIVE (Week 3 cloud-lite)
        ↓
Snowflake X-Small + dbt  ← planned (Week 4–5)
        ↓
Streamlit / Grafana dashboard  ← planned (Week 6)
```

## What runs where

| Stage | Runtime | Storage |
|-------|---------|---------|
| Ingest + medallion transforms | Local Spark / Redpanda | `data/bronze`, `data/silver`, `data/gold` |
| Durable cloud copy | — | S3 `gold/` prefix only (~55 MB for 1M demo) |
| Warehouse + marts | — (planned) | Snowflake |

## S3 cloud-lite (implemented)

- **Terraform:** `infra/aws/` — bucket, lifecycle, upload IAM user, budget alert
- **Upload:** `make upload-gold-s3` — `src/utils/upload_gold_to_s3.py`
- **Policy:** Upload user can write `gold/*` only; raw/bronze/silver never leave the laptop

Details: [cloud_lite_s3.md](cloud_lite_s3.md)

## Design principles

- Heavy processing runs locally (Spark).
- Only curated gold tables go to S3 and (later) Snowflake.
- Full 285M-event replay is documented but not run by default.

Full guardrails: [cost_controls.md](cost_controls.md)
