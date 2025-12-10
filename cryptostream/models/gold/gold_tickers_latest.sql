-- models/gold/gold_tickers_latest.sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['symbol'],
    partition_by={"field": "event_ts", "data_type": "timestamp", "granularity": "day"},
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
    price_change,
    price_change_percent,
    base_volume_24h,
    quote_volume_24h,
    trade_count_24h
  from {{ ref('silver_tickers') }}

  {% if is_incremental() %}
    -- reprocessa só uma janelinha recente para cada símbolo
    where event_ts >= timestamp_sub(
      (select max(event_ts) from {{ this }}),
      interval 30 minute
    )
  {% endif %}
),

ranked as (
  select
    *,
    row_number() over (
      partition by symbol
      order by event_time_ws desc
    ) as rn
  from base
)

select
  symbol,
  event_ts,
  event_time_ws,
  last_price,
  best_bid_price,
  best_ask_price,
  best_ask_price - best_bid_price as spread,
  (best_bid_price + best_ask_price) / 2 as mid_price,
  price_change,
  price_change_percent,
  base_volume_24h,
  quote_volume_24h,
  trade_count_24h
from ranked
where rn = 1
