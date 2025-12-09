{{ config(materialized='view') }}

select
  -- Metadados do envelope
  lower(s) as stream, -- Errado aqui, esperando a Ju corrigir
  ingest_ts,
  event_ts,

  -- Cabeçalho do evento
  json_value(payload, "$.e") as event_type, -- "kline"
  timestamp_millis(cast(json_value(payload, "$.E") as int64)) as event_time_ws,
  upper(json_value(payload, "$.s")) as symbol,

  -- Objeto kline "k"
  timestamp_millis(cast(json_value(payload, "$.k.t") as int64)) as open_time,
  timestamp_millis(cast(json_value(payload, "$.k.T") as int64)) as close_time,

  upper(json_value(payload, "$.k.s")) as kline_symbol,
  json_value(payload, "$.k.i") as kline_interval,

  cast(json_value(payload, "$.k.f") as int64) as first_trade_id,
  cast(json_value(payload, "$.k.L") as int64) as last_trade_id,

  cast(json_value(payload, "$.k.o") as numeric) as open_price,
  cast(json_value(payload, "$.k.c") as numeric) as close_price,
  cast(json_value(payload, "$.k.h") as numeric) as high_price,
  cast(json_value(payload, "$.k.l") as numeric) as low_price,

  cast(json_value(payload, "$.k.v") as numeric) as base_volume,
  cast(json_value(payload, "$.k.q") as numeric) as quote_volume,
  cast(json_value(payload, "$.k.n") as int64)   as trade_count,
  cast(json_value(payload, "$.k.x") as bool)    as is_closed,

  cast(json_value(payload, "$.k.V") as numeric) as taker_buy_base_volume,
  cast(json_value(payload, "$.k.Q") as numeric) as taker_buy_quote_volume,

  -- Campo B (ignore) também mapeado para completar o payload
  cast(json_value(payload, "$.k.B") as int64)   as ignore_kline

from {{ source('bronze', 'raw_candles') }}
