# dbt — CommerceStream

Presentation-layer transforms on **curated gold only**. No raw, bronze, or silver in Snowflake.

## Environment

Use **`.venv-dbt`** only (not the main `.venv` / Python 3.14):

```bash
# From repo root
set -a && . ./.env && set +a
.venv-dbt/bin/dbt debug --project-dir dbt/commercestream --profiles-dir ~/.dbt
make dbt-build   # preferred — suspends warehouse after
```

## Models

| Layer | Models |
|-------|--------|
| Sources | `gold_staging.fct_*` / `agg_*` in `COMMERCESTREAM_DB.STAGING` |
| Staging | `stg_fct_sessions`, `stg_fct_purchases`, `stg_agg_*`, `stg_fct_cart_abandonment` |
| Marts | `mart_sessions`, `mart_product_performance`, `mart_conversion_funnel` |

## Cost controls

- `threads: 1`, warehouse `DE_PROJECT_WH` (XSMALL)
- Prefer `dbt build --select <model>` while developing
- Full `dbt build` for milestone demos only
- **Always** `make snowflake-suspend` after dbt (Makefile does this)
