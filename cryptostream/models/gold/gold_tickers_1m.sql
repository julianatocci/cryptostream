-- models/gold/gold_tickers_1m.sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['symbol', 'bucket_ts'],
    partition_by={"field": "bucket_ts", "data_type": "timestamp", "granularity": "day"},
    cluster_by=["symbol"]
) }}

with base as (
  select
    symbol,
    event_ts,
    event_time_ws,
    last_price,
    best_bid_price,
    best_ask_price,
    price_change_percent,
    base_volume_24h,
    quote_volume_24h
  from {{ ref('silver_tickers') }}

  {% if is_incremental() %}
    where event_ts >= timestamp_sub(
      (select max(bucket_ts) from {{ this }}),
      interval 30 minute
    )
  {% endif %}
),

bucketed as (
  select
    symbol,
    timestamp_trunc(event_ts, minute) as bucket_ts,
    event_time_ws,
    last_price,
    best_bid_price,
    best_ask_price,
    price_change_percent,
    base_volume_24h,
    quote_volume_24h
  from base
),

-- pegamos o último ticker do minuto (mais recente event_time_ws)
agg as (
  select
    symbol,
    bucket_ts,
    array_agg(struct(
      event_time_ws,
      last_price,
      best_bid_price,
      best_ask_price,
      price_change_percent,
      base_volume_24h,
      quote_volume_24h
    )
    order by event_time_ws desc
    limit 1
    )[offset(0)] as s
  from bucketed
  group by 1, 2
)

select
  symbol,
  bucket_ts,
  s.last_price,
  s.best_bid_price,
  s.best_ask_price,
  s.best_ask_price - s.best_bid_price as spread,
  (s.best_ask_price + s.best_bid_price) / 2 as mid_price,
  s.price_change_percent,
  s.base_volume_24h,
  s.quote_volume_24h
from agg
