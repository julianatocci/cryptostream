{{ config(materialized='view') }}

select
  -- Metadados do envelope
  lower(s) as stream, -- Errado aqui, esperando a Ju corrigir
  ingest_ts,
  event_ts,

  -- Cabeçalho do evento
  json_value(payload, "$.e") as event_type,
  timestamp_millis(cast(json_value(payload, "$.E") as int64)) as event_time_ws,
  upper(json_value(payload, "$.s")) as symbol,

  -- Estatísticas de preço 24h
  cast(json_value(payload, "$.p") as numeric) as price_change,
  cast(json_value(payload, "$.P") as numeric) as price_change_percent,
  cast(json_value(payload, "$.w") as numeric) as weighted_avg_price,
  cast(json_value(payload, "$.x") as numeric) as first_trade_before_24h_price,

  cast(json_value(payload, "$.c") as numeric) as last_price,
  cast(json_value(payload, "$.Q") as numeric) as last_qty,

  -- Melhor bid/ask
  cast(json_value(payload, "$.b") as numeric) as best_bid_price,
  cast(json_value(payload, "$.B") as numeric) as best_bid_qty,
  cast(json_value(payload, "$.a") as numeric) as best_ask_price,
  cast(json_value(payload, "$.A") as numeric) as best_ask_qty,

  -- OHLC 24h
  cast(json_value(payload, "$.o") as numeric) as open_price_24h,
  cast(json_value(payload, "$.h") as numeric) as high_price_24h,
  cast(json_value(payload, "$.l") as numeric) as low_price_24h,

  -- Volumes 24h
  cast(json_value(payload, "$.v") as numeric) as base_volume_24h,
  cast(json_value(payload, "$.q") as numeric) as quote_volume_24h,

  -- Janela estatística
  timestamp_millis(cast(json_value(payload, "$.O") as int64)) as stats_open_time,
  timestamp_millis(cast(json_value(payload, "$.C") as int64)) as stats_close_time,
  cast(json_value(payload, "$.F") as int64) as first_trade_id_24h,
  cast(json_value(payload, "$.L") as int64) as last_trade_id_24h,
  cast(json_value(payload, "$.n") as int64) as trade_count_24h

from {{ source('bronze', 'raw_tickers') }}
