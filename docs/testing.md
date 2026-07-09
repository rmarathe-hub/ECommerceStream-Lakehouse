# Testing and quality gates

This document describes how to validate the **local** Weeks 1–2 pipeline. Week 3 cloud work (Terraform, S3 upload, Snowflake, dbt) has **not** started.

## Data locality

| Layer | Location | Cloud upload |
|-------|----------|--------------|
| Raw samples | `data/raw/events_*.csv` | Never |
| Bronze | `data/bronze/` | Never |
| Silver | `data/silver/` | Never |
| Gold marts | `data/gold/` | Future Week 3+ (curated gold only) |

Only `data/gold/` is eligible for future cloud upload. Raw, bronze, and silver remain local.

## Command comparison

| Command | Kafka replay | Resets state | Typical runtime | Purpose |
|---------|--------------|--------------|-----------------|---------|
| `make quick-test` | 10k events | No | ~30–60 sec | Fast ingest smoke test |
| `make local-demo-100k` | 100k events | No | ~3–5 min | Week 1 streaming demo |
| `make verify-1m` | No | No | ~30–60 sec | Re-validate existing 1M outputs |
| `make quality-gate` | No | No | ~2–3 min | Full Weeks 1–2 production-quality checks |
| `make local-demo-1m` | 1M events | Yes (`reset-demo-state`) | ~25–35 min | Full medallion demo from scratch |

### `make quality-gate`

Runs the local quality gate orchestrator (`scripts/run_local_quality_gate.sh`):

1. **Python syntax** — `compileall` on `src/` and `scripts/`
2. **Docker stack** — polls health via `scripts/wait_for_stack.sh` (not `docker compose wait`)
3. **Git data hygiene** — confirms no CSV/Parquet/lake data is tracked
4. **Sample files** — row counts, columns, `event_id` uniqueness, `event_type` validity
5. **Bronze/silver/gold** — layer existence, Kafka metadata columns, dedup behavior
6. **`make verify-1m`** — full `validate_pipeline.py` with 1M row minimums
7. **Milestone comparison** — compares counts/revenue against known 1M demo targets

Safe defaults:

- Does **not** replay Kafka
- Does **not** run `reset-demo-state`
- Does **not** upload to S3 or touch Snowflake
- Does **not** run the 5M stress demo

Options (via shell script):

```bash
./scripts/run_local_quality_gate.sh --skip-docker      # skip stack health poll
./scripts/run_local_quality_gate.sh --skip-pipeline    # skip make verify-1m
```

Or run the Python gate directly:

```bash
.venv/bin/python3 src/validation/run_quality_gate.py --skip-pipeline
```

### `make verify-1m`

Runs `validate_pipeline.py` with `MIN_BRONZE_ROWS=1000000` and `MIN_SILVER_ROWS=1000000`. Validates all layers and cross-layer reconciliation. Writes `data/gold/dq_pipeline_summary.json`.

Does not start Docker, replay Kafka, or reset state. Use after a successful `make local-demo-1m`.

### Known 1M milestone (clean state)

After `make local-demo-1m` with `reset-demo-state`:

| Metric | Expected |
|--------|----------|
| Bronze / silver rows | 1,000,000 each |
| fct_sessions | ~874,457 |
| fct_purchases | ~17,405 |
| agg_product_performance | ~83,600 |
| fct_cart_abandonment | ~20,858 |
| Total revenue | ~$5,377,910.49 |
| validate-pipeline | OVERALL: PASSED |

### Cumulative bronze / silver dedup

- **Bronze grows cumulatively** if Kafka is replayed without `reset-demo-state` or `stream-bronze-reset`.
- **Silver dedupes by `event_id`** (latest Kafka offset wins), so silver row count can be lower than bronze after repeated produces.
- For a clean 1M demo, always use `make local-demo-1m` (which calls `reset-demo-state`) or run `reset-demo-state` manually before replay.

### Producer performance

`replay_events.py` uses batch flush (default 1000 msgs) and async acks — not per-message blocking. Expected throughput: **~650–675 events/sec** at 1M scale. `local-demo-1m` runtime: **~25–35 minutes** (mostly produce step).

## Prerequisites

```bash
make venv
make up          # for quick-test / local-demo targets that need Kafka
make sample-1m   # if data/raw/events_1m.csv is missing
```

For `make quality-gate` on an existing 1M pipeline, Docker should be running but Kafka replay is not required.

## Week 3 status

**Day 18 complete (scaffold only).** AWS monthly cost budget with email alerts is defined in `infra/aws/budget.tf`. Add `budget_alert_emails` to `terraform.tfvars` before apply. Do **not** `apply` until Day 20.
