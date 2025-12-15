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

  -- Estatísticas de preço 24h
  cast(json_value(payload, "$.data.p") as numeric) as price_change,
  cast(json_value(payload, "$.data.P") as numeric) as price_change_percent,
  cast(json_value(payload, "$.data.w") as numeric) as weighted_avg_price,
  cast(json_value(payload, "$.data.x") as numeric) as first_trade_before_24h_price,

  cast(json_value(payload, "$.data.c") as numeric) as last_price,
  cast(json_value(payload, "$.data.Q") as numeric) as last_qty,

  -- Melhor bid/ask
  cast(json_value(payload, "$.data.b") as numeric) as best_bid_price,
  cast(json_value(payload, "$.data.B") as numeric) as best_bid_qty,
  cast(json_value(payload, "$.data.a") as numeric) as best_ask_price,
  cast(json_value(payload, "$.data.A") as numeric) as best_ask_qty,

  -- OHLC 24h
  cast(json_value(payload, "$.data.o") as numeric) as open_price_24h,
  cast(json_value(payload, "$.data.h") as numeric) as high_price_24h,
  cast(json_value(payload, "$.data.l") as numeric) as low_price_24h,

  -- Volumes 24h
  cast(json_value(payload, "$.data.v") as numeric) as base_volume_24h,
  cast(json_value(payload, "$.data.q") as numeric) as quote_volume_24h,

  -- Janela estatística
  timestamp_millis(cast(json_value(payload, "$.data.O") as int64)) as stats_open_time,
  timestamp_millis(cast(json_value(payload, "$.data.C") as int64)) as stats_close_time,
  cast(json_value(payload, "$.data.F") as int64) as first_trade_id_24h,
  cast(json_value(payload, "$.data.L") as int64) as last_trade_id_24h,
  cast(json_value(payload, "$.data.n") as int64) as trade_count_24h

from {{ source('bronze', 'raw_tickers') }}
