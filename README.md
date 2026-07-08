# ECommerceStream-Lakehouse

A cost-controlled real-time e-commerce lakehouse pipeline. Replays 1M–5M events through a local streaming stack (Redpanda/Kafka → Spark Structured Streaming) into bronze/silver/gold Parquet layers, with curated outputs loaded to S3 and Snowflake for dbt marts and dashboards.

**Snowflake budget:** $2–6/month, hard cap at $6.

> **Cost controls are mandatory.** Read [docs/cost_controls.md](docs/cost_controls.md) before writing code or using Snowflake.

## Project structure

```
ECommerceStream-Lakehouse/
├── README.md
├── Makefile
├── docker-compose.yml
├── .env.example
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── src/
│   ├── producer/
│   ├── streaming/
│   ├── transforms/
│   ├── validation/
│   └── utils/
├── airflow/
│   └── dags/
├── dbt/
│   └── commercestream/
├── infra/
│   ├── aws/
│   └── snowflake/
├── sql/
│   ├── admin/
│   └── snowflake/
├── dashboards/
│   └── streamlit/
├── monitoring/
│   ├── prometheus/
│   └── grafana/
└── docs/
    ├── architecture.md
    ├── cost_controls.md
    ├── data_dictionary.md
    ├── demo_strategy.md
    └── build_plan.md
```

## Quick start

```bash
cp .env.example .env   # fill in values as needed
python3 -m venv .venv  # one-time setup
.venv/bin/pip install -r requirements.txt
make help
make up              # start Redpanda, Spark, MinIO
make ps              # verify containers are healthy
make logs            # tail logs (Ctrl+C to exit)
make down            # stop stack
```

### Local stack (Week 1)

| Service       | URL / Port                         | Notes                              |
|---------------|------------------------------------|------------------------------------|
| Redpanda Kafka| `localhost:19092`                  | Host producer connects here        |
| Redpanda (Docker) | `redpanda:9092`                | Spark jobs inside Docker use this  |
| Schema Registry | `localhost:18081`                | Optional                           |
| Spark Master UI | http://localhost:8080            | Cluster dashboard                  |
| Spark Master  | `spark://localhost:7077`           | `spark-submit` from host           |
| MinIO S3 API  | http://localhost:9000              | S3-compatible local object store   |
| MinIO Console | http://localhost:9001              | Login: `minioadmin` / `minioadmin` |
| Postgres      | `localhost:5432`                   | Optional — `docker compose --profile airflow up -d` |

`./data` and `./src` are mounted into Spark containers at `/opt/data` and `/opt/src` for streaming jobs.

### Dataset setup (Week 1)

1. Download a monthly file from [Kaggle e-commerce behavior data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) into `data/raw/source/` (start with `2019-Oct.csv` or `2019-Oct.csv.gz`).
2. Create sampled files locally:

```bash
make sample-1m   # -> data/raw/events_1m.csv
make sample-5m   # -> data/raw/events_5m.csv  (use multiple source months if needed)
```

Raw source files and samples stay on disk only — they are gitignored and never uploaded to S3 or Snowflake.

### Demo strategy (1M cloud / 5M local)

| Demo | File | Events | Cloud (S3 / Snowflake) |
|------|------|--------|-------------------------|
| **1M cloud-lite** | `data/raw/events_1m.csv` | 1M | Yes — curated gold only |
| **5M local stress** | `data/raw/events_5m.csv` | 5M | **No** — local pipeline only |
| **285M full replay** | — | 285M | **Never** — documented only |

The 5M run proves scale locally without a second Snowflake reload. See [docs/demo_strategy.md](docs/demo_strategy.md).

### Event replay (Week 1)

With the Docker stack running (`make up`), replay sampled events into Redpanda:

```bash
.venv/bin/pip install -r requirements.txt   # if not already installed
make produce-100k
```

Or directly:

```bash
python3 src/producer/replay_events.py \
  --input data/raw/events_1m.csv \
  --topic ecommerce_events \
  --limit 100000 \
  --rate-per-second 1000
```

### Bronze ingestion (Week 1)

Stream Kafka events into partitioned bronze Parquet:

```bash
make stream-bronze        # process new Kafka offsets
make stream-bronze-reset  # wipe checkpoint and reprocess from earliest
```

Output:

- `data/bronze/events/` — partitioned by `event_date`, `event_type`
- `data/bronze/quarantine/` — invalid records with `invalid_reason`

Validate bronze output:

```bash
make validate-bronze
```

Full 100k smoke test (produce → stream → validate):

```bash
make smoke-test-100k
```

## Build plan

Full day-by-day plan: [docs/build_plan.md](docs/build_plan.md)

| Week | Focus                                      |
|------|--------------------------------------------|
| 0    | Project setup and cost guardrails          |
| 1    | Local streaming foundation (+ **Day 3.5 producer optimization** before bronze) |
| 2    | Silver/gold Spark transformations          |
| 3    | Terraform + S3 cloud-lite                  |
| 4    | Snowflake cost guardrails                  |
| 5    | Snowflake load + dbt                       |
| 6    | Monitoring, dashboard, CI, polish            |

**Week 1 Day 3.5:** Optimize `replay_events.py` (batch/async acks) **before Day 4** — cuts 100k replay to ~1.7 min and 5M from ~19 hours → ~1.4 hours. Day 5 smoke test verifies speed.
