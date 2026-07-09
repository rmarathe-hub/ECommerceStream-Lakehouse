select * from {{ source('gold_staging', 'agg_product_performance') }}
