select
    session_id,
    user_id,
    session_date,
    session_start_ts,
    session_end_ts,
    session_duration_seconds,
    view_count,
    cart_count,
    remove_from_cart_count,
    cart_event_count,
    distinct_products_carted,
    first_cart_ts,
    last_cart_ts,
    abandoned,
    gold_processed_at
from {{ source('gold_staging', 'fct_cart_abandonment') }}
