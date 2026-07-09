# dbt — CommerceStream (Day 27 scaffold)

Presentation-layer transforms on **curated gold only**. No raw, bronze, or silver in Snowflake.

## Status

| Phase | Action |
|-------|--------|
| Day 27 | Project scaffold — **do not run `dbt build`** |
| Week 5 | Load gold into `STAGING`, then `dbt build --select <subset>` |

## Setup (when ready for Week 5)

```bash
pip install dbt-snowflake   # optional; not in base requirements.txt yet
cp profiles.yml.example ~/.dbt/profiles.yml   # or merge profiles
# Export Snowflake env vars from .env before dbt commands
set -a && . ../../.env && set +a
dbt debug --project-dir .    # Week 5 only
```

## Models

| Layer | Models |
|-------|--------|
| Staging | `stg_fct_sessions`, `stg_fct_purchases`, `stg_agg_*`, `stg_fct_cart_abandonment` |
| Marts | `mart_sessions`, `mart_product_performance`, `mart_conversion_funnel` |

Sources point to `COMMERCESTREAM_DB.STAGING` tables created by Week 5 `COPY INTO`.

## Cost controls

- `threads: 1`, `DE_PROJECT_WH` (XSMALL)
- `dbt build --select <model>` while developing
- Full `dbt build` only for milestone demos
- **Always** `make snowflake-suspend` after dbt
