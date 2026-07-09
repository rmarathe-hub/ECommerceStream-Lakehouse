-- Week 5: refine columns after gold load; scaffold uses passthrough.
select * from {{ ref('stg_agg_conversion_funnel') }}
