-- Bootstrap Snowflake cost guardrails (no data load).
-- Run from repo root:
--   snowsql -a "$SNOWFLAKE_ACCOUNT" -u "$SNOWFLAKE_USER" -r "$SNOWFLAKE_ROLE" \
--     -f sql/admin/run_guardrails_in_order.sql
-- Or: make snowflake-guardrails

!source sql/admin/01_create_cost_controlled_warehouse.sql
!source sql/admin/02_create_resource_monitor.sql
!source sql/admin/03_create_database_schemas.sql
!source sql/admin/04_suspend_warehouse.sql
!source sql/admin/05_check_snowflake_guardrails.sql
