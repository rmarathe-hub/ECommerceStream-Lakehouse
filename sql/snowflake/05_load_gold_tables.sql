-- Load curated gold Parquet from S3 stage into COMMERCESTREAM_DB.STAGING.
-- Gold only — no raw, bronze, or silver.
-- Hive partition columns (session_date / purchase_date) are parsed from METADATA$FILENAME.
-- Always end the session with: make snowflake-suspend

USE WAREHOUSE DE_PROJECT_WH;
USE DATABASE COMMERCESTREAM_DB;
USE SCHEMA STAGING;

-- Parquet format for COPY INTO (created here so load does not depend on Day 26 file-format object)
CREATE FILE FORMAT IF NOT EXISTS COMMERCESTREAM_DB.STAGING.COMMERCESTREAM_PARQUET_FF
  TYPE = PARQUET
  COMPRESSION = AUTO
  COMMENT = 'Parquet format for Spark gold marts uploaded to S3';

-- External stage (verified): COMMERCESTREAM_DB.RAW.S3_GOLD_STAGE → s3://.../gold/

-- ---------------------------------------------------------------------------
-- fct_sessions (partition: session_date)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE COMMERCESTREAM_DB.STAGING.fct_sessions (
  session_id STRING,
  user_id NUMBER(38, 0),
  session_date DATE,
  session_start_ts TIMESTAMP_NTZ,
  session_end_ts TIMESTAMP_NTZ,
  session_duration_seconds NUMBER(38, 0),
  event_count NUMBER(38, 0),
  view_count NUMBER(38, 0),
  cart_count NUMBER(38, 0),
  remove_from_cart_count NUMBER(38, 0),
  purchase_count NUMBER(38, 0),
  distinct_products_viewed NUMBER(38, 0),
  distinct_products_purchased NUMBER(38, 0),
  session_revenue FLOAT,
  converted BOOLEAN,
  gold_processed_at TIMESTAMP_NTZ
);

COPY INTO COMMERCESTREAM_DB.STAGING.fct_sessions
FROM (
  SELECT
    $1:session_id::STRING,
    $1:user_id::NUMBER,
    TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'session_date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1, 1, 'e', 1)),
    $1:session_start_ts::TIMESTAMP_NTZ,
    $1:session_end_ts::TIMESTAMP_NTZ,
    $1:session_duration_seconds::NUMBER,
    $1:event_count::NUMBER,
    $1:view_count::NUMBER,
    $1:cart_count::NUMBER,
    $1:remove_from_cart_count::NUMBER,
    $1:purchase_count::NUMBER,
    $1:distinct_products_viewed::NUMBER,
    $1:distinct_products_purchased::NUMBER,
    $1:session_revenue::FLOAT,
    $1:converted::BOOLEAN,
    $1:gold_processed_at::TIMESTAMP_NTZ
  FROM @COMMERCESTREAM_DB.RAW.S3_GOLD_STAGE/fct_sessions/
)
FILE_FORMAT = (FORMAT_NAME = COMMERCESTREAM_DB.STAGING.COMMERCESTREAM_PARQUET_FF)
PATTERN = '.*[.]parquet'
ON_ERROR = 'ABORT_STATEMENT';

-- ---------------------------------------------------------------------------
-- fct_purchases (partition: purchase_date)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE COMMERCESTREAM_DB.STAGING.fct_purchases (
  purchase_id STRING,
  purchase_ts TIMESTAMP_NTZ,
  purchase_date DATE,
  session_id STRING,
  user_id NUMBER(38, 0),
  product_id NUMBER(38, 0),
  category_id NUMBER(38, 0),
  category_code STRING,
  brand STRING,
  purchase_amount FLOAT,
  event_seq_in_session NUMBER(38, 0),
  seconds_from_session_start NUMBER(38, 0),
  bronze_ingested_at TIMESTAMP_NTZ,
  silver_processed_at TIMESTAMP_NTZ,
  gold_processed_at TIMESTAMP_NTZ
);

COPY INTO COMMERCESTREAM_DB.STAGING.fct_purchases
FROM (
  SELECT
    $1:purchase_id::STRING,
    $1:purchase_ts::TIMESTAMP_NTZ,
    TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'purchase_date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1, 1, 'e', 1)),
    $1:session_id::STRING,
    $1:user_id::NUMBER,
    $1:product_id::NUMBER,
    $1:category_id::NUMBER,
    $1:category_code::STRING,
    $1:brand::STRING,
    $1:purchase_amount::FLOAT,
    $1:event_seq_in_session::NUMBER,
    $1:seconds_from_session_start::NUMBER,
    $1:bronze_ingested_at::TIMESTAMP_NTZ,
    $1:silver_processed_at::TIMESTAMP_NTZ,
    $1:gold_processed_at::TIMESTAMP_NTZ
  FROM @COMMERCESTREAM_DB.RAW.S3_GOLD_STAGE/fct_purchases/
)
FILE_FORMAT = (FORMAT_NAME = COMMERCESTREAM_DB.STAGING.COMMERCESTREAM_PARQUET_FF)
PATTERN = '.*[.]parquet'
ON_ERROR = 'ABORT_STATEMENT';

-- ---------------------------------------------------------------------------
-- agg_product_performance (no Hive partition)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE COMMERCESTREAM_DB.STAGING.agg_product_performance (
  product_id NUMBER(38, 0),
  category_id NUMBER(38, 0),
  category_code STRING,
  brand STRING,
  view_count NUMBER(38, 0),
  cart_count NUMBER(38, 0),
  remove_from_cart_count NUMBER(38, 0),
  purchase_count NUMBER(38, 0),
  unique_viewers NUMBER(38, 0),
  unique_cart_adders NUMBER(38, 0),
  unique_purchasers NUMBER(38, 0),
  total_revenue FLOAT,
  last_event_date DATE,
  view_to_purchase_rate FLOAT,
  cart_to_purchase_rate FLOAT,
  gold_processed_at TIMESTAMP_NTZ
);

COPY INTO COMMERCESTREAM_DB.STAGING.agg_product_performance
FROM (
  SELECT
    $1:product_id::NUMBER,
    $1:category_id::NUMBER,
    $1:category_code::STRING,
    $1:brand::STRING,
    $1:view_count::NUMBER,
    $1:cart_count::NUMBER,
    $1:remove_from_cart_count::NUMBER,
    $1:purchase_count::NUMBER,
    $1:unique_viewers::NUMBER,
    $1:unique_cart_adders::NUMBER,
    $1:unique_purchasers::NUMBER,
    $1:total_revenue::FLOAT,
    $1:last_event_date::DATE,
    $1:view_to_purchase_rate::FLOAT,
    $1:cart_to_purchase_rate::FLOAT,
    $1:gold_processed_at::TIMESTAMP_NTZ
  FROM @COMMERCESTREAM_DB.RAW.S3_GOLD_STAGE/agg_product_performance/
)
FILE_FORMAT = (FORMAT_NAME = COMMERCESTREAM_DB.STAGING.COMMERCESTREAM_PARQUET_FF)
PATTERN = '.*[.]parquet'
ON_ERROR = 'ABORT_STATEMENT';

-- ---------------------------------------------------------------------------
-- agg_conversion_funnel (partition: session_date)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE COMMERCESTREAM_DB.STAGING.agg_conversion_funnel (
  session_date DATE,
  total_sessions NUMBER(38, 0),
  sessions_with_view NUMBER(38, 0),
  sessions_with_cart NUMBER(38, 0),
  sessions_with_purchase NUMBER(38, 0),
  view_to_cart_sessions NUMBER(38, 0),
  cart_to_purchase_sessions NUMBER(38, 0),
  view_to_purchase_sessions NUMBER(38, 0),
  abandoned_cart_sessions NUMBER(38, 0),
  view_to_cart_rate FLOAT,
  cart_to_purchase_rate FLOAT,
  view_to_purchase_rate FLOAT,
  cart_abandonment_rate FLOAT,
  gold_processed_at TIMESTAMP_NTZ
);

COPY INTO COMMERCESTREAM_DB.STAGING.agg_conversion_funnel
FROM (
  SELECT
    TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'session_date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1, 1, 'e', 1)),
    $1:total_sessions::NUMBER,
    $1:sessions_with_view::NUMBER,
    $1:sessions_with_cart::NUMBER,
    $1:sessions_with_purchase::NUMBER,
    $1:view_to_cart_sessions::NUMBER,
    $1:cart_to_purchase_sessions::NUMBER,
    $1:view_to_purchase_sessions::NUMBER,
    $1:abandoned_cart_sessions::NUMBER,
    $1:view_to_cart_rate::FLOAT,
    $1:cart_to_purchase_rate::FLOAT,
    $1:view_to_purchase_rate::FLOAT,
    $1:cart_abandonment_rate::FLOAT,
    $1:gold_processed_at::TIMESTAMP_NTZ
  FROM @COMMERCESTREAM_DB.RAW.S3_GOLD_STAGE/agg_conversion_funnel/
)
FILE_FORMAT = (FORMAT_NAME = COMMERCESTREAM_DB.STAGING.COMMERCESTREAM_PARQUET_FF)
PATTERN = '.*[.]parquet'
ON_ERROR = 'ABORT_STATEMENT';

-- ---------------------------------------------------------------------------
-- fct_cart_abandonment (partition: session_date)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE COMMERCESTREAM_DB.STAGING.fct_cart_abandonment (
  session_id STRING,
  user_id NUMBER(38, 0),
  session_date DATE,
  session_start_ts TIMESTAMP_NTZ,
  session_end_ts TIMESTAMP_NTZ,
  session_duration_seconds NUMBER(38, 0),
  view_count NUMBER(38, 0),
  cart_count NUMBER(38, 0),
  remove_from_cart_count NUMBER(38, 0),
  cart_event_count NUMBER(38, 0),
  distinct_products_carted NUMBER(38, 0),
  first_cart_ts TIMESTAMP_NTZ,
  last_cart_ts TIMESTAMP_NTZ,
  abandoned BOOLEAN,
  gold_processed_at TIMESTAMP_NTZ
);

COPY INTO COMMERCESTREAM_DB.STAGING.fct_cart_abandonment
FROM (
  SELECT
    $1:session_id::STRING,
    $1:user_id::NUMBER,
    TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, 'session_date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1, 1, 'e', 1)),
    $1:session_start_ts::TIMESTAMP_NTZ,
    $1:session_end_ts::TIMESTAMP_NTZ,
    $1:session_duration_seconds::NUMBER,
    $1:view_count::NUMBER,
    $1:cart_count::NUMBER,
    $1:remove_from_cart_count::NUMBER,
    $1:cart_event_count::NUMBER,
    $1:distinct_products_carted::NUMBER,
    $1:first_cart_ts::TIMESTAMP_NTZ,
    $1:last_cart_ts::TIMESTAMP_NTZ,
    $1:abandoned::BOOLEAN,
    $1:gold_processed_at::TIMESTAMP_NTZ
  FROM @COMMERCESTREAM_DB.RAW.S3_GOLD_STAGE/fct_cart_abandonment/
)
FILE_FORMAT = (FORMAT_NAME = COMMERCESTREAM_DB.STAGING.COMMERCESTREAM_PARQUET_FF)
PATTERN = '.*[.]parquet'
ON_ERROR = 'ABORT_STATEMENT';
