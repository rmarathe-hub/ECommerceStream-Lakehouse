# Demo Strategy

This project uses two demo tiers to balance **scale proof** with **cost control**.

## Demo matrix

| Demo | Events | Pipeline scope | S3 upload | Snowflake / dbt |
|------|--------|----------------|-----------|-----------------|
| **1M cloud-lite** | 1,000,000 | Full local + cloud | Yes (gold only) | Yes (curated gold) |
| **5M local stress** | 5,000,000 | Full local only | **No** | **No** |
| **285M full replay** | 285,000,000 | Documented only | **No** | **No** |

## Why 5M stays local

- Proves the streaming and medallion pipeline at higher volume
- Keeps Snowflake spend in the **$2–6/month** band
- Cloud receives **curated gold from the 1M demo**, not a second large reload
- Recruiters care about architecture and tradeoffs, not dumping 5M raw events into a warehouse

## Commands

```bash
# Sample files (Week 1)
make sample-1m    # -> data/raw/events_1m.csv
make sample-5m    # -> data/raw/events_5m.csv

# 1M end-to-end (Weeks 1–6, includes cloud-lite)
make local-demo-1m        # planned
make cloud-lite           # planned — upload gold, load Snowflake, dbt, suspend

# 5M local stress test (Week 2+, no cloud)
make local-demo-5m        # planned — producer, bronze, silver, gold, validate only
```

## Resume framing

> Replayed 1M–5M e-commerce events through a local Kafka/Spark streaming lakehouse; loaded **curated gold outputs only** from the 1M demo to S3 and Snowflake under strict cost guardrails.
