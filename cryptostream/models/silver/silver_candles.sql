{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['symbol', 'kline_interval', 'open_time'],
    partition_by={"field": "event_ts", "data_type": "timestamp", "granularity": "hour"},
    cluster_by=["symbol"]
) }}

with stg as (
  select *
  from {{ ref('staging_candles') }}
),

filtered as (
  select *
  from stg
  where symbol is not null
    and kline_interval is not null
    and open_time is not null
    and close_time is not null
    and open_price is not null
    and high_price is not null
    and low_price is not null
    and close_price is not null
    and open_price > 0
    and high_price > 0
    and low_price > 0
    and close_price > 0
),

deduped as (
  select * except(rn)
  from (
    select
      *,
      row_number() over (
        partition by symbol, kline_interval, open_time
        order by ingest_ts desc
      ) as rn
    from filtered
  )
  where rn = 1
)

select
  stream,
  ingest_ts,
  event_ts,
  event_time_ws,

  symbol,
  kline_interval,
  open_time,
  close_time,

  open_price,
  high_price,
  low_price,
  close_price,

  base_volume,
  quote_volume,
  trade_count,
  first_trade_id,
  last_trade_id,
  is_closed,
  taker_buy_base_volume,
  taker_buy_quote_volume,
  ignore_kline,

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
