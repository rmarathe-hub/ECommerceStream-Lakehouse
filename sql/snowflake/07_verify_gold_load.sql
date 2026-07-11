-- Verify gold staging tables after COPY INTO (row counts only).

USE WAREHOUSE DE_PROJECT_WH;
USE DATABASE COMMERCESTREAM_DB;
USE SCHEMA STAGING;

SELECT 'fct_sessions' AS table_name, COUNT(*) AS row_count FROM COMMERCESTREAM_DB.STAGING.fct_sessions
UNION ALL
SELECT 'fct_purchases', COUNT(*) FROM COMMERCESTREAM_DB.STAGING.fct_purchases
UNION ALL
SELECT 'agg_product_performance', COUNT(*) FROM COMMERCESTREAM_DB.STAGING.agg_product_performance
UNION ALL
SELECT 'agg_conversion_funnel', COUNT(*) FROM COMMERCESTREAM_DB.STAGING.agg_conversion_funnel
UNION ALL
SELECT 'fct_cart_abandonment', COUNT(*) FROM COMMERCESTREAM_DB.STAGING.fct_cart_abandonment
ORDER BY table_name;
