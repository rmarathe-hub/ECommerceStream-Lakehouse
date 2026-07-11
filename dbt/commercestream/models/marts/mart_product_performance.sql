{{ config(materialized='table') }}

select
    product_id,
    category_code,
    brand,
    view_count,
    cart_count,
    purchase_count,
    unique_purchasers,
    total_revenue,
    view_to_purchase_rate,
    cart_to_purchase_rate,
    last_event_date,
    gold_processed_at
from {{ ref('stg_agg_product_performance') }}
