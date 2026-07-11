{% docs __overview__ %}
# CommerceStream dbt project

Presentation-layer transforms on **curated gold only**.

- Sources: `COMMERCESTREAM_DB.STAGING` tables loaded from S3 `gold/` via Snowflake `COPY INTO`
- Staging models: thin views (`stg_*`)
- Marts: dashboard tables in `MARTS` (`mart_*`)

**Cost controls:** `DE_PROJECT_WH` (XSMALL), `threads: 1`, always `make snowflake-suspend` after dbt.

Use `.venv-dbt` for all dbt commands — not the main `.venv`.
{% enddocs %}
