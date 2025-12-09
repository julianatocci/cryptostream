{{ config(materialized='view') }}

select
  -- Metadados do envelope
  lower(stream) as stream,
  ingest_ts,
  event_ts,

  -- Campos do payload do trade stream <symbol>@trade
  json_value(payload, "$.e") as event_type, -- "trade"
  timestamp_millis(cast(json_value(payload, "$.E") as int64)) as event_time_ws,

  upper(json_value(payload, "$.s")) as symbol,

  cast(json_value(payload, "$.t") as int64)  as trade_id,
  cast(json_value(payload, "$.p") as numeric) as price,
  cast(json_value(payload, "$.q") as numeric) as qty,

  timestamp_millis(cast(json_value(payload, "$.T") as int64)) as trade_ts,

  cast(json_value(payload, "$.m") as bool) as is_buyer_maker,
  cast(json_value(payload, "$.M") as bool) as ignore_flag -- campo "M" (ignore) também mapeado

from {{ source('bronze', 'raw_trades') }}
