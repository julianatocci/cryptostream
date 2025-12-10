{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['symbol','bucket_ts'],
    partition_by={"field":"bucket_ts","data_type":"timestamp","granularity":"day"},
    cluster_by=["symbol"]
) }}

with base as (
  select *
  from {{ ref('gold_trades_ohlcv_1m') }}

  {% if is_incremental() %}
    where bucket_ts >= timestamp_sub(
      (select max(bucket_ts) from {{ this }}),
      interval 2 day
    )
  {% endif %}
),

bucketed as (
  select
    symbol,
    timestamp_trunc(bucket_ts, hour) as bucket_ts,
    bucket_ts as minute_ts_in_bucket,
    open, high, low, close,
    volume, n_trades
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

    sum(volume)   as volume,
    sum(n_trades) as n_trades
  from bucketed
  group by 1,2
)

select * from agg
