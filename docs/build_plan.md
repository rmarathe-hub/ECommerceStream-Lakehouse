# Build Plan

Week-by-week plan for ECommerceStream-Lakehouse. See [cost_controls.md](cost_controls.md) and [demo_strategy.md](demo_strategy.md) for guardrails and demo tiers.

## Week 0: Setup and cost guardrails

| Day | Task | Status |
|-----|------|--------|
| 0.1 | Project structure, Makefile, docker-compose skeleton | Done |
| 0.2 | `docs/cost_controls.md` — Snowflake $2–6/month, hard cap $6 | Done |

## Week 1: Local streaming foundation

| Day | Task | Status |
|-----|------|--------|
| 1 | Docker Compose: Redpanda, Spark, MinIO | Done |
| 2 | Event sampling (`sample_events.py`, `make sample-1m` / `sample-5m`) | Done |
| 3 | Kafka replay producer (`replay_events.py`, `make produce-100k`) | Done |
| **3.5** | **Producer throughput optimization** (before bronze work) | Done |
| 4 | Spark Structured Streaming bronze writer (`kafka_to_bronze.py`) | Done |
| 5 | 100k smoke test + bronze validation (verify fast replay ~2.5 min) | Done |
| 6 | `local-demo-100k` Makefile target + README docs | Done |
| 7 | Buffer/fix day (Docker, Spark, bronze bugs) | Done |

### Day 3.5 — Producer throughput optimization (before Day 4)

**Goal:** Speed up Kafka replay from ~72 events/sec to **500–1000 events/sec** using batch/async acks — **before** building the bronze writer so Day 4 iteration is fast.

**Why now (not Day 7):**

- Day 4 bronze development needs multiple `produce-100k` runs; ~25 min each is too slow
- Day 5 smoke test verifies both bronze **and** fast replay (~1.7 min for 100k)
- Optimization is independent of bronze logic — low risk to do early
- Day 7 becomes buffer/fix only

| Run | Before (~72/sec) | After (~1000/sec) |
|-----|------------------|-------------------|
| 100k | ~25 min | ~1.7 min |
| 1M | ~4 hours | ~17 min |
| 5M | ~19 hours | ~1.4 hours |

**Prerequisite:** Day 3 producer works (initial `produce-100k` completes on slow path).

**Scope (keep small):**

- Remove per-message `future.get()` blocking acks in `src/producer/replay_events.py`
- Batch flush every N messages (e.g. 500–1000) with `producer.flush()`
- Keep `--rate-per-second` as optional throttle
- Verify: `make produce-100k` finishes in **~2.5 min** instead of ~25 min (~660 events/sec measured)

**Deliverables:**

- Updated `src/producer/replay_events.py`
- Optional Makefile targets: `produce-1m`, `produce-5m` (for Day 13 / Day 41 demos)

**Commit message:** `Optimize event replay producer with batch async acks`

### Day 5 — Verification checkpoint

When running the full smoke test, confirm:

- `make produce-100k` completes in **~2–3 min** (not ~25 min; ~660 events/sec measured)
- `make stream-bronze` writes bronze Parquet
- `make validate-bronze` passes all checks

### Day 7 — Buffer/fix only

Use for Docker, Spark, bronze, and producer **bug fixes** found during Days 4–6. Producer optimization should already be done on Day 3.5.

**Deliverables (Day 7):**

- `make up` uses `scripts/wait_for_stack.sh` health polling (not `docker compose wait`)
- Persistent `spark_ivy` Docker volume — Kafka connector JARs cached across restarts
- Fast dev targets: `produce-10k`, `smoke-test-10k`, `quick-test`
- `make status` alias for `make ps`
- [docs/troubleshooting.md](troubleshooting.md) — Week 1 runbook

### Day 6 — Local 100k demo command

`make local-demo-100k` chains `up` → `produce-100k` → `stream-bronze` → `validate-bronze` for a single-command Week 1 demo.

## Week 2: Silver/gold Spark transformations

| Day | Task | Status |
|-----|------|--------|
| 8 | Bronze → silver cleaning | Done |
| 9 | Sessionization | Done |
| 10 | Purchase and product marts | Done |
| 11 | Funnel and cart abandonment marts | Done |
| 12 | Data quality checks | Done |
| 13 | Full local **1M** demo (uses optimized producer from Day 3.5) | Done |
| 14 | Buffer/fix day | Done |

### Day 8 — Bronze → silver cleaning

**Goal:** Batch Spark job that normalizes bronze events into a deduplicated silver table.

**Deliverables:**

- `src/transforms/bronze_to_silver.py` — parse timestamps, normalize strings, filter invalid rows, dedupe by `event_id`
- `src/validation/validate_silver.py` — silver DQ checks (unique `event_id`, required fields, non-negative price)
- `make transform-silver`, `make validate-silver`, `make smoke-test-silver`
- Output: `data/silver/events/` partitioned by `event_date`

### Day 9 — Sessionization

**Goal:** Enrich silver events with per-session ordering and build the `fct_sessions` gold mart.

**Deliverables:**

- `src/transforms/silver_sessionize.py` — event sequence in session, session timing metrics, session aggregates
- `src/validation/validate_sessions.py` — reconcile row counts and session-level DQ checks
- `make transform-sessions`, `make validate-sessions`, `make smoke-test-sessions`
- Outputs:
  - `data/silver/session_events/` — events with `event_seq_in_session`, `seconds_from_session_start`
  - `data/gold/fct_sessions/` — one row per session with counts, revenue, conversion flag

### Day 10 — Purchase and product marts

**Goal:** Build purchase fact and product performance gold tables from sessionized events.

**Deliverables:**

- `src/transforms/build_purchase_product_marts.py` — `fct_purchases` and `agg_product_performance`
- `src/validation/validate_purchase_marts.py` — row-count and revenue reconciliation checks
- `make transform-purchase-marts`, `make validate-purchase-marts`, `make smoke-test-purchase-marts`
- Outputs:
  - `data/gold/fct_purchases/` — one row per purchase event
  - `data/gold/agg_product_performance/` — one row per product with funnel metrics and revenue

### Day 11 — Funnel and cart abandonment marts

**Goal:** Build daily conversion funnel aggregates and a cart abandonment fact table.

**Deliverables:**

- `src/transforms/build_funnel_marts.py` — ordered session funnel flags, daily aggregates, abandoned carts
- `src/validation/validate_funnel_marts.py` — reconcile session counts and abandonment logic
- `make transform-funnel-marts`, `make validate-funnel-marts`, `make smoke-test-funnel-marts`
- Outputs:
  - `data/gold/agg_conversion_funnel/` — daily view → cart → purchase funnel metrics
  - `data/gold/fct_cart_abandonment/` — sessions with cart but no purchase

### Day 12 — Data quality checks

**Goal:** Unified pipeline DQ validation with cross-layer reconciliation.

**Deliverables:**

- `src/validation/validate_pipeline.py` — orchestrates all layer validators + revenue/session reconciliation
- `make validate-pipeline`, `make validate-gold`, `make transform-gold`
- JSON summary report: `data/gold/dq_pipeline_summary.json`

### Day 13 — Full local 1M demo

**Goal:** One-command end-to-end local medallion demo at 1M event scale.

**Deliverables:**

- `make local-demo-1m` — up → reset → produce-1m → bronze → silver → gold → validate-pipeline
- `make reset-demo-state` — wipe Kafka topic and pipeline Parquet for a clean rerun
- `validate-pipeline` accepts `--min-bronze-rows` / `--min-silver-rows` for milestone checks

**Typical runtime:** ~25–35 minutes (mostly `produce-1m` at ~1000 events/sec).

### Day 14 — Buffer/fix only

Use for Docker, Spark, silver/gold, and demo **bug fixes** found during Days 8–13.

**Deliverables (Day 14):**

- Fix `make up` hang — replace `docker compose wait` with `scripts/wait_for_stack.sh`
- Fix Spark Ivy cache permissions on `stream-bronze` / `stream-bronze-reset`
- `make verify-1m` — re-validate existing 1M pipeline without Kafka replay (~1 min)
- Updated [docs/troubleshooting.md](troubleshooting.md) — stack wait, Ivy, silver dedup, timing guide
- `minio-init` marked `restart: "no"` in docker-compose

## Week 3: Terraform + S3 cloud-lite

S3 bucket, IAM, lifecycle rules, budget alert, gold-only upload. No Snowflake yet.

| Day | Task | Status |
|-----|------|--------|
| 15 | Terraform S3 bucket scaffold (`infra/aws/`) | Done (scaffold only — no `apply`) |
| 16 | IAM least-privilege | Done (scaffold only — no `apply`) |
| 17 | S3 lifecycle rules (refine) | Done (scaffold only — no `apply`) |
| 18 | AWS budget alert | Planned |
| 19 | `upload_gold_to_s3.py` + `make upload-gold-s3` | Planned |
| 20 | `terraform apply` + S3 smoke test | Planned |
| 21 | Buffer/fix — no raw uploads | Planned |

### Day 15 — Terraform S3 bucket scaffold

**Goal:** Define S3 infrastructure as code without creating AWS resources yet.

**Deliverables:**

- `infra/aws/` — `provider.tf`, `variables.tf`, `s3.tf`, `outputs.tf`, `terraform.tfvars.example`, `README.md`
- S3 bucket with public access blocked, AES256 encryption, ownership controls, lifecycle rules for `temp/`, `checkpoints/`, `bronze/sample/`
- Outputs: `bucket_name`, `bucket_arn`, `gold_prefix`
- `.gitignore` updated for `*.tfvars` (except example) and Terraform state

**Safe commands only:** `terraform fmt`, `terraform init`, `terraform plan` — **no `terraform apply`**.

**Upload policy:** Future `make upload-gold-s3` uploads **only** `data/gold/`. Raw, bronze, and silver remain local.

**Snowflake:** Week 4 only, after cost guardrails are verified.

### Day 16 — IAM least-privilege upload user

**Goal:** Dedicated IAM user for curated gold S3 uploads with minimal permissions.

**Deliverables:**

- `infra/aws/iam.tf` — upload user, scoped policy, policy attachment, optional access key
- Policy allows `gold/*` (Get/Put only), `temp/*` and `checkpoints/*` (Put/Delete)
- Policy denies implicit access to `raw/`, `bronze/`, `silver/`, and other buckets
- Outputs: `upload_user_name`, `upload_policy_arn`, `upload_access_key_id`, `upload_secret_access_key` (sensitive)
- `.env.example` updated with `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` placeholders

**Safe commands only:** `terraform fmt`, `init`, `validate`, `plan` — **no `terraform apply`**.

**Day 20:** Run `terraform apply` manually, then copy access keys to local `.env` only.

### Day 17 — S3 lifecycle rules

**Goal:** Parameterize and document S3 lifecycle expiration for cost control.

**Deliverables:**

- Lifecycle variables in `variables.tf` with validation (`lifecycle_*_expiration_days`)
- `s3.tf` uses variables for `temp/` (1d), `checkpoints/` (7d), `bronze/sample/` (30d)
- `gold/` retained with no expiration rule
- `lifecycle_rules` output in `outputs.tf`
- Updated `infra/aws/README.md` prefix/lifecycle table

**Safe commands only:** `terraform fmt`, `init`, `validate`, `plan` — **no `terraform apply`**.

## Week 4: Snowflake cost guardrails

X-Small warehouse, auto-suspend, resource monitor, database/schemas — **before any data load**.

## Week 5: Snowflake load + dbt

Curated gold only. Explicit `suspend-snowflake` after every run.

## Week 6: Monitoring, dashboard, CI, polish

Streamlit/Grafana, manual GitHub Actions, final **1M cloud-lite** demo.

| Day | Task |
|-----|------|
| 40 | Final 1M cloud-lite demo run |
| 41 | Optional **5M local stress** demo (no Snowflake reload) |
| 42 | README polish + resume bullets |
