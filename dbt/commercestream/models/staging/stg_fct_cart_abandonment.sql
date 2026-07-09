select * from {{ source('gold_staging', 'fct_cart_abandonment') }}
