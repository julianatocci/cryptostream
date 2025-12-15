{{ config(materialized='view') }}

select
  -- Metadados do envelope
  lower(json_value(payload, "$.stream")) as stream,
  ingest_ts,
  event_ts,

  -- Campos do payload do trade stream (AGORA VIA $.data.*)
  json_value(payload, "$.data.e") as event_type, -- "trade"
  timestamp_millis(cast(json_value(payload, "$.data.E") as int64)) as event_time_ws,

  upper(json_value(payload, "$.data.s")) as symbol,

  cast(json_value(payload, "$.data.t") as int64)  as trade_id,
  cast(json_value(payload, "$.data.p") as numeric) as price,
  cast(json_value(payload, "$.data.q") as numeric) as qty,

  timestamp_millis(cast(json_value(payload, "$.data.T") as int64)) as trade_ts,

  cast(json_value(payload, "$.data.m") as bool) as is_buyer_maker,
  cast(json_value(payload, "$.data.M") as bool) as ignore_flag

from {{ source('bronze', 'raw_trades') }}
