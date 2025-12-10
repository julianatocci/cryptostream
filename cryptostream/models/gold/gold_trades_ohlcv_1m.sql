{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['symbol','bucket_ts'],
    partition_by={"field":"bucket_ts","data_type":"timestamp","granularity":"day"},
    cluster_by=["symbol"]
) }}

with trades as (
  select
    symbol,
    event_ts as ts,
    price,
    qty
  from {{ ref('silver_trades') }}

  {% if is_incremental() %}
    -- reprocessa uma janelinha pra pegar trades atrasados
    where event_ts >= timestamp_sub(
      (select max(bucket_ts) from {{ this }}),
      interval 20 minute
    )
  {% endif %}
),

bucketed as (
  select
    symbol,
    timestamp_trunc(ts, minute) as bucket_ts,
    price,
    qty,
    ts
  from trades
),

agg as (
  select
    symbol,
    bucket_ts,

    -- open = primeiro preço do minuto
    array_agg(price order by ts asc limit 1)[offset(0)] as open,
    -- high/low dentro do minuto
    max(price) as high,
    min(price) as low,
    -- close = último preço do minuto
    array_agg(price order by ts desc limit 1)[offset(0)] as close,

    sum(qty) as volume,
    count(*) as n_trades
  from bucketed
  group by 1,2
)

select * from agg
