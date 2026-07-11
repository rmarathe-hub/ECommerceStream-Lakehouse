# Streamlit marts dashboard

Thin dashboard over **Snowflake `MARTS` only** (`mart_sessions`, `mart_product_performance`, `mart_conversion_funnel`).

## Setup

```bash
# Prefer .venv-dbt (already has Snowflake connectivity) or a dedicated venv
.venv-dbt/bin/pip install -r dashboards/streamlit/requirements.txt
```

Requires repo-root `.env` with `SNOWFLAKE_*` credentials.

## Run

```bash
make dashboard
# When finished:
make snowflake-suspend
```

Opens http://localhost:8501. Queries resume `DE_PROJECT_WH` (XSMALL, auto-suspend 60s).

## Cost notes

- Reads curated marts only — no raw/bronze/silver
- Results cached 5 minutes in Streamlit
- Always suspend the warehouse when you close the dashboard
