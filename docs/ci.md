# Continuous integration

Manual cloud jobs and cheap local checks. **No scheduled Snowflake runs.**

## Workflows

| Workflow | Trigger | What it does |
|----------|---------|--------------|
| `local-quality` | push / PR / manual | `compileall`, required-file checks, `dbt parse` (no warehouse) |
| `cloud-lite` | **manual only** (`workflow_dispatch`) | Guardrails check → optional gold load → optional dbt → **always suspend** |

## GitHub secrets (cloud-lite)

Set these in the repo → Settings → Secrets and variables → Actions:

| Secret | Purpose |
|--------|---------|
| `SNOWFLAKE_ACCOUNT` | Account identifier |
| `SNOWFLAKE_USER` | User |
| `SNOWFLAKE_PASSWORD` | Password |
| `SNOWFLAKE_ROLE` | Role (e.g. `ACCOUNTADMIN`) |

Never commit secrets. Local `.env` stays gitignored.

## Run cloud-lite manually

1. Actions → **cloud-lite** → Run workflow
2. Choose inputs:
   - `check_guardrails_only` — cheapest smoke check
   - `run_dbt` — rebuild marts (default on)
   - `run_load` — `COPY INTO` gold (costs credits; default off)
3. Confirm the final step **Suspend DE_PROJECT_WH** ran (`if: always()`)

## Local helper (same SQL runner CI uses)

```bash
.venv-dbt/bin/python scripts/run_snowflake_sql.py sql/admin/05_check_snowflake_guardrails.sql
.venv-dbt/bin/python scripts/run_snowflake_sql.py --ignore-errors sql/admin/04_suspend_warehouse.sql
```

## Cost rules

- No `on: schedule` for Snowflake
- Warehouse suspend runs even when earlier steps fail
- Prefer local `make cloud-lite` for full demos; use Actions for occasional verification
