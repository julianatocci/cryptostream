{{ config(materialized='view') }}

select
  -- Metadados do envelope
  lower(json_value(payload, "$.stream")) as stream,
  ingest_ts,
  event_ts,

  -- Cabeçalho do evento
  json_value(payload, "$.data.e") as event_type,
  timestamp_millis(cast(json_value(payload, "$.data.E") as int64)) as event_time_ws,
  upper(json_value(payload, "$.data.s")) as symbol,

  -- Objeto kline "k"
  timestamp_millis(cast(json_value(payload, "$.data.k.t") as int64)) as open_time,
  timestamp_millis(cast(json_value(payload, "$.data.k.T") as int64)) as close_time,

  upper(json_value(payload, "$.data.k.s")) as kline_symbol,
  json_value(payload, "$.data.k.i") as kline_interval,

  cast(json_value(payload, "$.data.k.f") as int64) as first_trade_id,
  cast(json_value(payload, "$.data.k.L") as int64) as last_trade_id,

  cast(json_value(payload, "$.data.k.o") as numeric) as open_price,
  cast(json_value(payload, "$.data.k.c") as numeric) as close_price,
  cast(json_value(payload, "$.data.k.h") as numeric) as high_price,
  cast(json_value(payload, "$.data.k.l") as numeric) as low_price,

  cast(json_value(payload, "$.data.k.v") as numeric) as base_volume,
  cast(json_value(payload, "$.data.k.q") as numeric) as quote_volume,
  cast(json_value(payload, "$.data.k.n") as int64)   as trade_count,
  cast(json_value(payload, "$.data.k.x") as bool)    as is_closed,

  cast(json_value(payload, "$.data.k.V") as numeric) as taker_buy_base_volume,
  cast(json_value(payload, "$.data.k.Q") as numeric) as taker_buy_quote_volume,

  cast(json_value(payload, "$.data.k.B") as int64)   as ignore_kline

from {{ source('bronze', 'raw_candles') }}
