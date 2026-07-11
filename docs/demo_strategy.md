# Demo Strategy

This project uses two demo tiers to balance **scale proof** with **cost control**.

## Demo matrix

| Demo | Events | Pipeline scope | S3 upload | Snowflake / dbt |
|------|--------|----------------|-----------|-----------------|
| **1M cloud-lite** | 1,000,000 | Full local + cloud | **Yes** (gold only, verified) | **Yes** (load + dbt + Streamlit verified) |
| **5M local stress** | 5,000,000 | Full local only | **No** | **No** |
| **285M full replay** | 285,000,000 | Documented only | **No** | **No** |

## Why 5M stays local

- Proves the streaming and medallion pipeline at higher volume
- Keeps Snowflake spend in the **$2–6/month** band
- Cloud receives **curated gold from the 1M demo**, not a second large reload
- Recruiters care about architecture and tradeoffs, not dumping 5M raw events into a warehouse

## Producer throughput (Day 3.5 — before Day 4)

The Day 3 producer uses per-message sync acks (~72 events/sec on a laptop). **Day 3.5** (before bronze work) optimizes to batch/async acks targeting **500–1000 events/sec** — free, local only, and critical before Day 4 iteration and 1M/5M replays:

| Run | Before | After (target) |
|-----|--------|----------------|
| 100k | ~25 min | ~1.7 min |
| 1M | ~4 hours | ~17 min |
| 5M | ~19 hours | ~1.4 hours |

See [build_plan.md](build_plan.md#day-35--producer-throughput-optimization-before-day-4).

Day 5 smoke test verifies replay finishes in **~1.7 min** for 100k.

## Commands

```bash
# Sample files (Week 1)
make sample-1m    # -> data/raw/events_1m.csv
make sample-5m    # -> data/raw/events_5m.csv

# 1M end-to-end (Weeks 1–6, includes cloud-lite)
make local-demo-100k        # Week 1: up + produce + bronze + validate
make local-demo-1m          # Full local 1M: bronze + silver + gold + DQ (~25-35 min)
make verify-1m              # Re-check existing 1M outputs (~1 min, no replay)
make upload-gold-s3-dry-run # Preview gold upload (no S3 calls)
make upload-gold-s3         # Upload curated gold to S3 (verified: 225 files, ~55 MB)
make cloud-lite             # planned — upload gold + load Snowflake + dbt + suspend

# 5M local stress test (Week 2+, no cloud)
make local-demo-5m        # planned — producer, bronze, silver, gold, validate only
```

## Resume framing

> Replayed 1M–5M e-commerce events through a local Kafka/Spark streaming lakehouse; loaded **curated gold outputs only** from the 1M demo to S3 and Snowflake under strict cost guardrails.
