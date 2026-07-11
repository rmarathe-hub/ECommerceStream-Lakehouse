{{ config(materialized='table') }}

select
    session_date,
    total_sessions,
    sessions_with_view,
    sessions_with_cart,
    sessions_with_purchase,
    view_to_cart_rate,
    cart_to_purchase_rate,
    view_to_purchase_rate,
    cart_abandonment_rate,
    abandoned_cart_sessions,
    gold_processed_at
from {{ ref('stg_agg_conversion_funnel') }}
