select * from {{ source('gold_staging', 'agg_conversion_funnel') }}
