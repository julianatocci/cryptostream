{{ config(materialized='view') }}

select
  -- Metadados do envelope
  lower(stream) as stream,
  ingest_ts,
  event_ts,

  -- Campos do payload depthUpdate
  json_value(payload, "$.e") as event_type, -- "depthUpdate"
  timestamp_millis(cast(json_value(payload, "$.E") as int64)) as event_time_ws,
  upper(json_value(payload, "$.s")) as symbol,

  cast(json_value(payload, "$.U") as int64) as first_update_id,
  cast(json_value(payload, "$.u") as int64) as last_update_id,

  -- Arrays de níveis de preço como JSON
  json_query(payload, "$.b") as bids,  -- [[price, qty], ...]
  json_query(payload, "$.a") as asks   -- [[price, qty], ...]

from {{ source('bronze', 'raw_orderbook') }}
