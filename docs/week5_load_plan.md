# Week 5 load plan (dry-run)

Day 28 checklist before the first Snowflake data load. **No `COPY INTO` or `dbt build` in Week 4.**

## Prerequisites

| # | Check | Command / location |
|---|--------|-------------------|
| 1 | Local 1M gold exists | `make verify-1m` |
| 2 | Guardrails verified | `make snowflake-check-guardrails` |
| 3 | Gold on S3 | `make upload-gold-s3` (225 files, ~55 MB) |
| 4 | Storage IAM role | [infra/snowflake/README.md](../infra/snowflake/README.md) |
| 5 | Stage setup | `make snowflake-stage-setup` |
| 6 | dbt scaffold | `dbt/commercestream/` (Day 27) |

Automated dry-run (no Snowflake calls):

```bash
make week5-load-dry-run
```

## Week 5 session flow (planned)

```mermaid
flowchart LR
  A[upload-gold-s3] --> B[snowflake-stage-setup]
  B --> C[snowflake-load-gold]
  C --> D[dbt build subset]
  D --> E[snowflake-suspend]
```

1. **`make snowflake-check-guardrails`** — warehouse XSMALL, monitor attached, suspended
2. **`make upload-gold-s3`** — refresh S3 gold if needed
3. **`make snowflake-stage-setup`** — file format + integration + stage (Day 26)
4. **`make snowflake-load-gold`** — `COPY INTO` staging tables (Day 29, not yet implemented)
5. **`dbt build --select <model>`** — presentation marts in `MARTS` (Day 30+)
6. **`make snowflake-suspend`** — **always**, even on failure

## Credit budget (reminder)

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

Add to shell history / aliases if helpful. The Makefile chains suspend into future `cloud-lite` and `dbt-build` targets (Week 5).
