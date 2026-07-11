# Week 5 — Snowflake load + dbt (complete)

Curated gold from S3 → `COMMERCESTREAM_DB.STAGING` → dbt marts in `MARTS`.  
**No raw, bronze, or silver in Snowflake.**

## Verified results (1M cloud-lite)

| Check | Result |
|-------|--------|
| Stage `LIST` | 225 gold files on `@COMMERCESTREAM_DB.RAW.S3_GOLD_STAGE` |
| `make snowflake-load-gold` | All `COPY INTO` status `LOADED`, `errors_seen = 0` |
| `make dbt-build` | **51/51** PASS (5 views, 3 tables, 43 tests) |
| `make cloud-lite` | upload → load → dbt → suspend (~99s end-to-end re-run) |
| Warehouse | `DE_PROJECT_WH` suspended after every step |

### Staging row counts (after load)

| Table | Rows |
|-------|------|
| `fct_sessions` | 874,457 |
| `agg_product_performance` | 83,600 |
| `fct_cart_abandonment` | 20,858 |
| `fct_purchases` | 17,405 |
| `agg_conversion_funnel` | 31 |

### dbt marts

| Mart | Schema |
|------|--------|
| `mart_sessions` | `COMMERCESTREAM_DB.MARTS` |
| `mart_product_performance` | `COMMERCESTREAM_DB.MARTS` |
| `mart_conversion_funnel` | `COMMERCESTREAM_DB.MARTS` |

## Commands

```bash
make snowflake-check-guardrails
make snowflake-stage-list
make snowflake-load-gold      # COPY INTO + verify + suspend
make dbt-build                # .venv-dbt only + suspend
make cloud-lite               # upload → load → dbt → suspend
make snowflake-suspend        # always after any ad-hoc session
```

## Credit budget

| Session | Expected credits |
|---------|------------------|
| Stage setup | ~0.1 |
| Gold load | ~0.2–0.3 |
| dbt build | ~0.2–0.3 |

Hard cap: `DE_PROJECT_MONITOR` at 3 credits/month.

## Habit: suspend after every session

```bash
make snowflake-suspend
```

Makefile targets (`snowflake-load-gold`, `dbt-build`, `cloud-lite`) chain suspend automatically.
