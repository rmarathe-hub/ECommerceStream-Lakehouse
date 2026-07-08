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

- `make up` uses `docker compose up -d --wait` (replaces fixed `sleep` in demo)
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
| 10 | Purchase and product marts |
| 11 | Funnel and cart abandonment marts |
| 12 | Data quality checks |
| 13 | Full local **1M** demo (uses optimized producer from Day 3.5) |
| 14 | Buffer/fix day |

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

## Week 3: Terraform + S3 cloud-lite

S3 bucket, IAM, lifecycle rules, budget alert, gold-only upload. No Snowflake yet.

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
