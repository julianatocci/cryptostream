-- models/gold/gold_orderbook_liquidity_1m.sql
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
    minute_bucket,
    bids,
    asks
  from {{ ref('silver_orderbook') }}

  {% if is_incremental() %}
    where event_ts >= timestamp_sub(
      (select max(bucket_ts) from {{ this }}),
      interval 30 minute
    )
  {% endif %}
),

-- explode bids
bid_levels as (
  select
    symbol,
    minute_bucket as bucket_ts,
    'BID' as side,
    cast(json_value(level, '$[0]') as numeric) as price,
    cast(json_value(level, '$[1]') as numeric) as qty
  from base,
  unnest(json_extract_array(bids)) as level
),

-- explode asks
ask_levels as (
  select
    symbol,
    minute_bucket as bucket_ts,
    'ASK' as side,
    cast(json_value(level, '$[0]') as numeric) as price,
    cast(json_value(level, '$[1]') as numeric) as qty
  from base,
  unnest(json_extract_array(asks)) as level
),

levels as (
  select * from bid_levels
  union all
  select * from ask_levels
),

agg as (
  select
    symbol,
    bucket_ts,

    max(case when side = 'BID' then price end) as best_bid_price,
    min(case when side = 'ASK' then price end) as best_ask_price,

    sum(case when side = 'BID' then qty end) as total_bid_qty,
    sum(case when side = 'ASK' then qty end) as total_ask_qty
  from levels
  group by 1,2
)

select
  symbol,
  bucket_ts,
  best_bid_price,
  best_ask_price,
  best_ask_price - best_bid_price as spread,
  (best_bid_price + best_ask_price) / 2 as mid_price,
  total_bid_qty,
  total_ask_qty
from agg
