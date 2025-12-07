{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['symbol','trade_id'],
    partition_by={"field": "event_ts", "data_type": "timestamp", "granularity": "hour"},
    cluster_by=["symbol"]
) }}

with stg as (
  select *
  from {{ ref('staging_trades') }}
),

filtered as (
  select *
  from stg
  where symbol is not null
    and trade_id is not null
    and price is not null
    and qty is not null
    and price > 0
    and qty > 0
),

deduped as (
  select * except(rn)
  from (
    select
      *,
      row_number() over (
        partition by symbol, trade_id
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
  trade_ts,
  event_time_ws,

  symbol,
  trade_id,
  price,
  qty,
  is_buyer_maker,

  date(event_ts) as event_date,
  timestamp_trunc(event_ts, minute) as minute_bucket,
  timestamp_trunc(event_ts, hour) as hour_bucket

from deduped

{% if is_incremental() %}
  where event_ts >= timestamp_sub(
    (select max(event_ts) from {{ this }}),
    interval 10 minute
  )
{% endif %}
