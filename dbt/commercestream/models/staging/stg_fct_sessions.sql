-- Week 5: passthrough after COPY INTO loads gold Parquet into STAGING.
select * from {{ source('gold_staging', 'fct_sessions') }}
