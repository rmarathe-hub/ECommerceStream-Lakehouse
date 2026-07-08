# Data Dictionary

> Placeholder — schema definitions will be added as bronze/silver/gold layers are built in Weeks 1–2.

## Planned layers

| Layer  | Location              | Description                          |
|--------|-----------------------|--------------------------------------|
| Raw    | `data/raw/`           | Sampled CSV event files (1M / 5M)    |
| Bronze | `data/bronze/`        | Kafka-consumed events as Parquet     |
| Silver | `data/silver/`        | Cleaned, deduplicated events         |
| Gold   | `data/gold/`          | Session, funnel, and revenue marts   |
