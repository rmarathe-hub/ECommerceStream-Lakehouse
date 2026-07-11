select
    purchase_id,
    purchase_ts,
    purchase_date,
    session_id,
    user_id,
    product_id,
    category_id,
    category_code,
    brand,
    purchase_amount,
    event_seq_in_session,
    seconds_from_session_start,
    bronze_ingested_at,
    silver_processed_at,
    gold_processed_at
from {{ source('gold_staging', 'fct_purchases') }}
