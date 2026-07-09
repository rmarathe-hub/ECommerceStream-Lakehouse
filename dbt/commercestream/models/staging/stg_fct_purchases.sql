select * from {{ source('gold_staging', 'fct_purchases') }}
