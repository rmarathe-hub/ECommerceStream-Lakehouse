# Architecture

> Placeholder — detailed architecture diagram and data flow will be added after the Week 1 bronze smoke test.

## Planned flow

```
1M–5M e-commerce events
        ↓
Python event replay producer
        ↓
Redpanda/Kafka (local)
        ↓
Spark Structured Streaming (local)
        ↓
Bronze / Silver / Gold Parquet (local)
        ↓
S3 curated gold outputs only
        ↓
Snowflake X-Small warehouse
        ↓
dbt marts & tests
        ↓
Streamlit / Grafana dashboard
```

## Design principles

- Heavy processing runs locally (Spark).
- Only curated gold tables go to S3 and Snowflake.
- Full 285M-event replay is documented but not run by default.

Full guardrails: [cost_controls.md](cost_controls.md)
