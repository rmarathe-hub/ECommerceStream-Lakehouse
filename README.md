# ECommerceStream-Lakehouse

A **cost-controlled** real-time e-commerce lakehouse: replay events locally through Kafka + Spark (bronze → silver → gold), then push **only curated gold marts** to S3 and Snowflake for dbt and a Streamlit dashboard.

**Snowflake budget:** $2–6/month · hard cap **$6** (3-credit resource monitor).

> Cost controls are mandatory. Read [docs/cost_controls.md](docs/cost_controls.md) before using Snowflake.

---

## Case study

### Problem

Show a production-shaped streaming lakehouse on a laptop budget: process ~1M e-commerce events end-to-end, land analytics in the cloud, and keep Snowflake spend under **~$6/month** — without loading raw event firehoses into the warehouse.

### Approach

| Layer | Choice | Why |
|-------|--------|-----|
| Ingest / transforms | Local Redpanda + Spark | Heavy compute stays free on the laptop |
| Cloud object store | S3 `gold/` only (~55 MB) | Durable curated outputs; no bronze/silver upload |
| Warehouse | Snowflake X-Small | Presentation layer only |
| Transforms in cloud | dbt (`threads: 1`) | Thin marts over loaded gold |
| Cost hard stop | Resource monitor 3 credits/mo | Suspend at 100% / 110% |
| Demo tiers | 1M cloud-lite · 5M local-only | Scale proof without a second cloud reload |

```
Events → Kafka → Spark (bronze/silver/gold)
                      ↓
              S3 gold/ (~55 MB)
                      ↓
         Snowflake STAGING → dbt MARTS
                      ↓
              Streamlit dashboard
```

### Verified results (1M cloud-lite)

| Checkpoint | Result |
|------------|--------|
| Local pipeline | 1M bronze/silver · ~874k sessions · ~17k purchases |
| S3 upload | **225 files · 54.86 MB** → `gold/` |
| Snowflake load | 874,457 sessions · 17,405 purchases · 83,600 products · 0 load errors |
| dbt | **51/51** tests passed · 3 mart tables |
| One-shot path | `make cloud-lite` (~99s re-run: upload → load → dbt → suspend) |
| Guardrails | XSMALL · auto-suspend 60s · monitor attached · warehouse suspended after runs |

### Cost tradeoffs

- **Do:** local Spark for medallion; upload gold only; XSMALL + 60s suspend; dbt subsets while iterating.
- **Don't:** load raw/bronze/silver to Snowflake; resize above XSMALL; leave the warehouse running; reload 5M into the cloud.

Full design notes: [docs/architecture.md](docs/architecture.md) · [docs/demo_strategy.md](docs/demo_strategy.md) · [docs/week5_load_plan.md](docs/week5_load_plan.md)

---

## Resume bullets

- Built a cost-controlled streaming lakehouse (Kafka/Redpanda → Spark Structured Streaming → bronze/silver/gold) and validated a **1M-event** local pipeline (~874k sessions, ~17k purchases).
- Implemented **cloud-lite** delivery: Terraform S3 + least-privilege IAM, uploading **only curated gold** (~55 MB) — never raw, bronze, or silver.
- Provisioned Snowflake with **X-Small** warehouse, 60s auto-suspend, and a **3-credit/month** resource monitor; loaded gold via external stage and built dbt marts (**51/51** tests passing).
- Shipped a one-command path (`make cloud-lite`) and a Streamlit dashboard over Snowflake marts, with explicit warehouse suspend after every cloud session.

---

## Project structure

```
ECommerceStream-Lakehouse/
├── README.md
├── Makefile
├── docker-compose.yml
├── .env.example
├── data/                    # raw / bronze / silver / gold (gitignored payloads)
├── src/                     # producer, streaming, transforms, validation, utils
├── dbt/commercestream/      # staging + marts (use .venv-dbt)
├── infra/aws/               # Terraform S3, IAM, budget
├── infra/snowflake/         # storage integration IAM docs
├── sql/admin/               # warehouse + resource monitor guardrails
├── sql/snowflake/           # stage + COPY INTO gold load
├── dashboards/streamlit/    # marts dashboard
└── docs/
```

## Quick start

```bash
cp .env.example .env   # fill in values as needed
python3 -m venv .venv  # one-time setup
.venv/bin/pip install -r requirements.txt
make help
make up              # start Redpanda, Spark, MinIO
make ps              # verify containers are healthy
make down            # stop stack
```

### Local stack

| Service       | URL / Port                         | Notes                              |
|---------------|------------------------------------|------------------------------------|
| Redpanda Kafka| `localhost:19092`                  | Host producer connects here        |
| Redpanda (Docker) | `redpanda:9092`                | Spark jobs inside Docker use this  |
| Spark Master UI | http://localhost:8080            | Cluster dashboard                  |
| MinIO Console | http://localhost:9001              | Login: `minioadmin` / `minioadmin` |

`./data` and `./src` are mounted into Spark containers at `/opt/data` and `/opt/src`.

### Dataset setup

1. Download a monthly file from [Kaggle e-commerce behavior data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) into `data/raw/source/`.
2. Sample locally:

```bash
make sample-1m   # -> data/raw/events_1m.csv
make sample-5m   # -> data/raw/events_5m.csv  (local stress only; no cloud reload)
```

### Demo strategy

| Demo | Events | Cloud (S3 / Snowflake) |
|------|--------|-------------------------|
| **1M cloud-lite** | 1M | Yes — curated gold only (**verified**) |
| **5M local stress** | 5M | **No** — optional, deferred |
| **285M full replay** | 285M | **Never** — documented only |

See [docs/demo_strategy.md](docs/demo_strategy.md).

### Local pipeline (Weeks 1–2)

```bash
make local-demo-1m      # full 1M medallion demo
make verify-1m          # re-check existing outputs (~1 min)
make quality-gate       # local Weeks 1–2 quality gate
```

### Cloud-lite (Weeks 3–5) — verified

```bash
make upload-gold-s3              # gold only → S3
make snowflake-check-guardrails
make snowflake-load-gold         # COPY INTO STAGING + suspend
make dbt-build                   # .venv-dbt only + suspend
make cloud-lite                  # upload → load → dbt → suspend
```

| Guardrail | Value |
|-----------|-------|
| Warehouse | `DE_PROJECT_WH` (XSMALL) |
| Auto-suspend | 60s · auto-resume on |
| Monitor | `DE_PROJECT_MONITOR` · 3 credits/mo · notify 50/80% · suspend 100/110% |
| Load policy | Gold marts only — no raw/bronze/silver |

Details: [docs/cloud_lite_s3.md](docs/cloud_lite_s3.md) · [docs/week5_load_plan.md](docs/week5_load_plan.md) · [docs/cost_controls.md](docs/cost_controls.md)

### Dashboard (Week 6)

```bash
make dashboard-install   # streamlit + connector into .venv-dbt
make dashboard           # http://localhost:8501
make snowflake-suspend   # when finished viewing
```

See [dashboards/streamlit/README.md](dashboards/streamlit/README.md).

## Build plan

Full day-by-day plan: [docs/build_plan.md](docs/build_plan.md)

| Week | Focus | Status |
|------|--------|--------|
| 0–2 | Local streaming + silver/gold | Done |
| 3 | Terraform + S3 cloud-lite | Done |
| 4 | Snowflake cost guardrails | Done |
| 5 | Snowflake load + dbt | **Done + verified** |
| 6 | Dashboard, docs polish (5M optional later) | Dashboard + README done |


## Docs

| Doc | Topic |
|-----|--------|
| [architecture.md](docs/architecture.md) | End-to-end data flow |
| [cost_controls.md](docs/cost_controls.md) | Budget and Snowflake rules |
| [week5_load_plan.md](docs/week5_load_plan.md) | Load + dbt verified results |
| [cloud_lite_s3.md](docs/cloud_lite_s3.md) | S3 upload path |
| [demo_strategy.md](docs/demo_strategy.md) | 1M vs 5M vs 285M |
| [testing.md](docs/testing.md) | Local quality gates |
| [troubleshooting.md](docs/troubleshooting.md) | Common failures |
