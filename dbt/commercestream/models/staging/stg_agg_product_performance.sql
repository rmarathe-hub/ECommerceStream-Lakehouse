select
    product_id,
    category_id,
    category_code,
    brand,
    view_count,
    cart_count,
    remove_from_cart_count,
    purchase_count,
    unique_viewers,
    unique_cart_adders,
    unique_purchasers,
    total_revenue,
    last_event_date,
    view_to_purchase_rate,
    cart_to_purchase_rate,
    gold_processed_at
from {{ source('gold_staging', 'agg_product_performance') }}
