-- models/gold/gold_candles_1m_closed.sql
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
    -- usamos o open_time como "bucket_ts" da candle
    open_time as bucket_ts,
    open_time,
    close_time,
    open_price,
    high_price,
    low_price,
    close_price,
    base_volume,
    quote_volume,
    trade_count,
    is_closed,
    event_ts
  from {{ ref('silver_candles') }}
  where is_closed = true

  {% if is_incremental() %}
    and event_ts >= timestamp_sub(
      (select max(bucket_ts) from {{ this }}),
      interval 20 minute
    )
  {% endif %}
),

deduped as (
  -- se por algum motivo chegar mais de uma candle pra mesmo symbol/interval/open_time,
  -- mantemos a última pelo event_ts
  select * except(rn)
  from (
    select
      *,
      row_number() over (
        partition by symbol, bucket_ts
        order by event_ts desc
      ) as rn
    from base
  )
  where rn = 1
)

select
  symbol,
  bucket_ts,          -- equivalente ao start da candle
  open_time,
  close_time,

  open_price as open,
  high_price as high,
  low_price  as low,
  close_price as close,

  base_volume as volume,          -- volume em ativo base
  quote_volume,                   -- se quiser olhar em quote também
  trade_count as n_trades

from deduped
