select
    session_date,
    total_sessions,
    sessions_with_view,
    sessions_with_cart,
    sessions_with_purchase,
    view_to_cart_sessions,
    cart_to_purchase_sessions,
    view_to_purchase_sessions,
    abandoned_cart_sessions,
    view_to_cart_rate,
    cart_to_purchase_rate,
    view_to_purchase_rate,
    cart_abandonment_rate,
    gold_processed_at
from {{ source('gold_staging', 'agg_conversion_funnel') }}
