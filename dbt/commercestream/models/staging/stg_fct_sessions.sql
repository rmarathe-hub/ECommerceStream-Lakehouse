-- Staging passthrough: curated gold sessions loaded from S3.
select
    session_id,
    user_id,
    session_date,
    session_start_ts,
    session_end_ts,
    session_duration_seconds,
    event_count,
    view_count,
    cart_count,
    remove_from_cart_count,
    purchase_count,
    distinct_products_viewed,
    distinct_products_purchased,
    session_revenue,
    converted,
    gold_processed_at
from {{ source('gold_staging', 'fct_sessions') }}
