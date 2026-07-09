# Snowflake admin SQL (cost guardrails)

Bootstrap scripts for **DE_PROJECT_WH** and **COMMERCESTREAM_DB**. No data load.

## Objects created

| Object | Purpose |
|--------|---------|
| `DE_PROJECT_WH` | X-Small warehouse, auto-suspend 60s |
| `DE_PROJECT_MONITOR` | 3 credit/month quota, notify 50/80%, suspend 100/110% |
| `COMMERCESTREAM_DB` | Project database |
| Schemas | `RAW`, `STAGING`, `MARTS`, `MONITORING` |

## Run (from repo root)

```bash
# Requires .env with SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_ROLE, SNOWFLAKE_PASSWORD
make snowflake-guardrails
make snowflake-check-guardrails
make snowflake-suspend
```

Password is read from `.env` via `SNOWFLAKE_PASSWORD` (never commit `.env`).

`ALTER WAREHOUSE ... SUSPEND` is wrapped with SnowSQL `!set exit_on_error=false` when the warehouse is already suspended (`INITIALLY_SUSPENDED = TRUE`).

## Manual SnowSQL

```bash
set -a && . ./.env && set +a
export SNOWSQL_PWD="$SNOWFLAKE_PASSWORD"
snowsql -a "$SNOWFLAKE_ACCOUNT" -u "$SNOWFLAKE_USER" -r "$SNOWFLAKE_ROLE" \
  -f sql/admin/run_guardrails_in_order.sql
```

## Not in this phase

- No `COPY INTO`, no stages loaded with data
- No `dbt build` (Day 27 scaffold only)
- Raw, bronze, and silver never go to Snowflake
