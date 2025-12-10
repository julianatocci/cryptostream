{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['symbol', 'event_time_ws'],
    partition_by={"field": "event_ts", "data_type": "timestamp", "granularity": "hour"},
    cluster_by=["symbol"]
) }}

with stg as (
  select *
  from {{ ref('staging_tickers') }}
),

filtered as (
  select *
  from stg
  where symbol is not null
    and event_time_ws is not null
    and last_price is not null
    and last_price > 0
),

deduped as (
  select * except(rn)
  from (
    select
      *,
      row_number() over (
        partition by symbol, event_time_ws
        order by ingest_ts desc
      ) as rn
    from filtered
  )
  where rn = 1
)

select
  ingest_ts,
  event_ts,
  event_time_ws,

  symbol,

  price_change,
  price_change_percent,
  weighted_avg_price,
  first_trade_before_24h_price,

  last_price,
  last_qty,

  best_bid_price,
  best_bid_qty,
  best_ask_price,
  best_ask_qty,

  open_price_24h,
  high_price_24h,
  low_price_24h,

  base_volume_24h,
  quote_volume_24h,

  stats_open_time,
  stats_close_time,
  first_trade_id_24h,
  last_trade_id_24h,
  trade_count_24h,

  date(event_ts) as event_date,
  timestamp_trunc(event_ts, minute) as minute_bucket,
  timestamp_trunc(event_ts, hour)   as hour_bucket

from deduped

{% if is_incremental() %}
  where event_ts >= timestamp_sub(
    (select max(event_ts) from {{ this }}),
    interval 10 minute
  )
{% endif %}
