{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['symbol','bucket_ts'],
    partition_by={"field":"bucket_ts","data_type":"timestamp","granularity":"day"},
    cluster_by=["symbol"]
) }}

with base as (
  select *
  from {{ ref('gold_candles_closed_1m') }}

  {% if is_incremental() %}
    where bucket_ts >= timestamp_sub(
      (select max(bucket_ts) from {{ this }}),
      interval 30 day
    )
  {% endif %}
),

bucketed as (
  select
    symbol,
    -- janelas de 1 dia
    timestamp_trunc(bucket_ts, day) as bucket_ts,
    bucket_ts as minute_ts_in_bucket,
    open,
    high,
    low,
    close,
    volume,
    quote_volume,
    n_trades
  from base
),

agg as (
  select
    symbol,
    bucket_ts,

    array_agg(open  order by minute_ts_in_bucket asc  limit 1)[offset(0)] as open,
    max(high) as high,
    min(low)  as low,
    array_agg(close order by minute_ts_in_bucket desc limit 1)[offset(0)] as close,

    sum(volume)       as volume,
    sum(quote_volume) as quote_volume,
    sum(n_trades)     as n_trades
  from bucketed
  group by 1,2
)

select * from agg
