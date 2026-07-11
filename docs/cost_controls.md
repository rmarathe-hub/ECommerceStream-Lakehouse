# Cost Controls

**Read this before writing any code or touching cloud services.**

This project is designed to be impressive without being expensive. The default operating budget is **$2–6/month on Snowflake**, with a **hard cap at $6**. Heavy work runs locally; cloud services receive only curated, smaller outputs.

See also: [architecture.md](architecture.md)

---

## Core principle

| Layer              | Role                                      | Cost profile   |
|--------------------|-------------------------------------------|----------------|
| Local Spark        | Heavy streaming and transformations       | Free (laptop)  |
| S3                 | Durable storage for curated gold Parquet  | Cents/month    |
| Snowflake X-Small  | Final warehouse and dbt marts only        | $2–6/month     |
| dbt                | Controlled manual/limited runs            | Tied to WH     |

**Do not** put the full 285M raw dataset into Snowflake.  
**Do not** keep Snowflake running between sessions.  
**Do not** schedule hourly Snowflake jobs.

---

## Snowflake guardrails (mandatory)

These settings must be in place **before loading any data** into Snowflake.

**Bootstrap (no data load):**

```bash
make snowflake-guardrails        # create warehouse, monitor, database/schemas, suspend
make snowflake-check-guardrails  # SHOW warehouses, monitors, schemas
make snowflake-suspend           # explicit suspend after any session
```

SQL scripts: `sql/admin/` — see [sql/admin/README.md](../sql/admin/README.md).

### Guardrail summary (automated)

| Setting | Value |
|---------|-------|
| Warehouse | `DE_PROJECT_WH` only (one project warehouse) |
| Size | `XSMALL` |
| Auto-suspend | 60 seconds |
| Auto-resume | `TRUE` |
| Initially suspended | `TRUE` |
| Resource monitor | `DE_PROJECT_MONITOR` |
| Monthly credit quota | 3 credits |
| Notify | 50%, 80% |
| Suspend | 100% |
| Suspend immediate | 110% |
| Database | `COMMERCESTREAM_DB` |
| Schemas | `RAW`, `STAGING`, `MARTS`, `MONITORING` |

**Data policy:** Heavy processing stays local in Spark. Snowflake receives **only curated gold marts** (Week 5+). No raw, bronze, or silver data is loaded to Snowflake.

**Session rule:** Every workflow ends with `ALTER WAREHOUSE DE_PROJECT_WH SUSPEND` (`make snowflake-suspend`).

### 1. One X-Small warehouse only

```sql
CREATE OR REPLACE WAREHOUSE DE_PROJECT_WH
  WAREHOUSE_SIZE = 'XSMALL'
  ...
```

Never resize above X-Small. Never create a second warehouse for this project.

### 2. Auto-suspend after 60 seconds

```sql
AUTO_SUSPEND = 60
```

The warehouse suspends automatically after 60 seconds of inactivity.

### 3. Auto-resume enabled

```sql
AUTO_RESUME = TRUE
```

Queries resume the warehouse on demand; combined with auto-suspend, compute is only billed while active.

### 4. Warehouse starts suspended

```sql
INITIALLY_SUSPENDED = TRUE
```

After creation or manual suspend, the warehouse must not sit running idle.

### 5. Resource monitor caps monthly credits

```sql
CREATE OR REPLACE RESOURCE MONITOR DE_PROJECT_MONITOR
  WITH CREDIT_QUOTA = 3
  FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 50 PERCENT DO NOTIFY
    ON 80 PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND
    ON 110 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE DE_PROJECT_WH SET RESOURCE_MONITOR = DE_PROJECT_MONITOR;
```

Resource monitors suspend warehouses at the credit threshold. They do **not** cap all possible Snowflake charges (e.g. storage, cloud services), but warehouse compute is the main risk and this is the hard stop.

**Budget mapping:**

| Target            | Setting                          |
|-------------------|----------------------------------|
| Normal spend      | $2–6/month                       |
| Hard cap          | $6/month via `CREDIT_QUOTA = 3` |
| ~$2/credit        | `CREDIT_QUOTA = 3` → ~$6 cap   |

### 6. Heavy Spark processing runs locally

All bronze, silver, and gold transformations run on the local Docker/Spark stack. Snowflake is not used for raw event processing.

### 7. Only curated 1M–5M event outputs go to Snowflake

| Dataset              | Local | S3        | Snowflake   |
|----------------------|-------|-----------|-------------|
| Raw / bronze / silver| Yes   | No        | No          |
| Gold marts (1M demo) | Yes   | Yes       | Yes         |
| Gold marts (5M demo) | Yes   | **No**    | **No**      |
| Full 285M replay     | Doc only | No     | **Never**   |

Load only small curated tables: sessions, purchases, funnel aggregates, revenue by category, DQ summaries — not raw event streams.

See [demo_strategy.md](demo_strategy.md) for the 1M cloud-lite vs 5M local-only demo matrix.

### 8. GitHub Actions cloud workflow is manual by default

```yaml
on:
  workflow_dispatch:
```

No scheduled hourly or daily Snowflake jobs. If a weekly schedule is added later, it must be opt-in and documented.

### 9. Every cloud workflow suspends the warehouse at the end

```yaml
- name: Suspend Snowflake warehouse
  if: always()
  run: python scripts/run_snowflake_sql.py --ignore-errors sql/admin/04_suspend_warehouse.sql
```

See [ci.md](ci.md) for `local-quality` (push/PR) and manual `cloud-lite` workflows.

Every Makefile cloud target (`load-snowflake`, `dbt-build`, `cloud-lite`) must chain to `suspend-snowflake`.

### 10. Full 285M-event replay is documented but not run by default

The full dataset may be referenced in docs and architecture diagrams. Default demos use **1M** (primary) or **5M** (extended, local only). Do not wire 285M into Makefile targets or CI.

---

## AWS / S3 guardrails

- **One S3 bucket** provisioned via Terraform — no EMR, MWAA, Kinesis, or NAT Gateway.
- **Upload only `data/gold/`** — never upload `data/raw/`, bronze, or silver.
- **Lifecycle rules** (Terraform `infra/aws/s3.tf`):
  - `temp/` — delete after 1 day (configurable via `lifecycle_temp_expiration_days`)
  - `checkpoints/` — delete after 7 days (`lifecycle_checkpoints_expiration_days`)
  - `bronze/sample/` — delete after 30 days (`lifecycle_bronze_sample_expiration_days`)
  - `gold/` — no expiration (retained)
- **Budget alert** — $5/month default AWS budget via Terraform (`infra/aws/budget.tf`); email notifications at 50/80/100%

---

## dbt settings

```yaml
# profiles.yml
warehouse: DE_PROJECT_WH
threads: 1
client_session_keep_alive: false
```

- Use `dbt build --select <subset>` while developing.
- Run full `dbt build` only for milestone demos.
- Always run `make suspend-snowflake` after dbt.

---

## Allowed Snowflake sessions (monthly)

Treat Snowflake as a **presentation layer**, not a development environment. Aim for **≤6 sessions/month**:

| # | Session                    | Expected credits |
|---|----------------------------|------------------|
| 1 | Bootstrap + verify guards  | ~0.1–0.2         |
| 2 | Create stage / file format | ~0.1             |
| 3 | Load curated gold tables   | ~0.2–0.3         |
| 4 | Full `dbt build`           | ~0.2–0.3         |
| 5 | Final demo verification    | ~0.1             |
| 6 | One retry buffer           | ~0.2             |

**Total expected: ~0.6–1.2 credits (~$2–4).** Hard cap prevents exceeding ~2–3 credits (~$6).

---

## Daily Snowflake safety checklist

### Before running

```sql
SHOW WAREHOUSES LIKE 'DE_PROJECT_WH';
```

Confirm:

- `WAREHOUSE_SIZE` = XSMALL  
- `AUTO_SUSPEND` = 60  
- `AUTO_RESUME` = true  
- `RESOURCE_MONITOR` = DE_PROJECT_MONITOR  
- `STATE` = SUSPENDED (or will suspend after the run)

### During running — do NOT

- Run `SELECT *` on large tables repeatedly  
- Run `dbt build` every few minutes while iterating  
- Load the full 285M raw dataset  
- Resize the warehouse above X-Small  
- Leave the warehouse running between steps  

Validate locally first. Use Snowflake only to prove the final path.

### After running — always

```sql
ALTER WAREHOUSE DE_PROJECT_WH SUSPEND;
```

Then verify:

```sql
SHOW WAREHOUSES LIKE 'DE_PROJECT_WH';
-- STATE should be SUSPENDED
```

### Weekly usage check

```sql
SELECT
  WAREHOUSE_NAME,
  START_TIME,
  END_TIME,
  CREDITS_USED
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME = 'DE_PROJECT_WH'
ORDER BY START_TIME DESC;
```

---

## Makefile safety design

The safe path must be the easy path. Planned targets (Week 4–5):

```makefile
cloud-lite:
	make upload-gold-s3
	make load-snowflake
	make dbt-build
	make suspend-snowflake

load-snowflake:
	snowsql -f sql/snowflake/load_gold_tables.sql
	make suspend-snowflake

dbt-build:
	cd dbt/commercestream && dbt build
	make suspend-snowflake

suspend-snowflake:
	snowsql -f sql/admin/04_suspend_warehouse.sql

check-snowflake-guards:
	snowsql -f sql/admin/05_check_snowflake_guardrails.sql

check-snowflake-usage:
	snowsql -f sql/admin/check_warehouse_usage.sql
```

**Implemented Makefile targets (prefer these):**

```bash
make snowflake-load-gold   # COPY INTO + verify + suspend
make dbt-build             # .venv-dbt/bin/dbt build + suspend
make cloud-lite            # upload → load → dbt → suspend
make snowflake-suspend
```

Even if you forget to suspend manually, the command sequence should do it for you.

---

## What is cheap vs. what is not

| Cheap (by design)                    | Expensive (avoid)                          |
|--------------------------------------|--------------------------------------------|
| Local Docker / Spark / Redpanda      | Loading raw events into Snowflake          |
| Curated gold Parquet on S3           | Hourly scheduled dbt or load jobs          |
| X-Small warehouse, short runs        | Warehouse left running overnight           |
| Manual GitHub Actions                | 5M dataset reload to Snowflake             |
| 1M cloud-lite demo                   | EMR, MWAA, NAT Gateway, Kinesis             |
| Resource monitor at 2–3 credits      | Resizing warehouse above X-Small           |

---

## Enforcement order

1. **Week 0** — This document (you are here).  
2. **Weeks 1–2** — Build and validate entirely locally. No Snowflake.  
3. **Week 3** — S3 only; upload gold outputs. No Snowflake.  
4. **Week 4** — Create Snowflake guardrails; stage + dbt scaffold; verify before any load.  
5. **Week 5** — Load curated gold + dbt; suspend after every run.  
6. **Week 6** — Manual CI with `if: always()` suspend step.

**Rule: Do not load data into Snowflake until the X-Small warehouse, 60-second auto-suspend, auto-resume, and resource monitor are verified.**
