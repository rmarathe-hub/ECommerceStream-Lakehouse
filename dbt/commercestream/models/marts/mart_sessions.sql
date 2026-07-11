{{ config(materialized='table') }}

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
    purchase_count,
    session_revenue,
    converted,
    gold_processed_at
from {{ ref('stg_fct_sessions') }}
