{{ config(materialized='view') }}

select
  -- Metadados do envelope
  lower(json_value(payload, "$.stream")) as stream,
  ingest_ts,
  event_ts,

  -- Campos do payload depthUpdate (AGORA VIA $.data.*)
  json_value(payload, "$.data.e") as event_type,
  timestamp_millis(cast(json_value(payload, "$.data.E") as int64)) as event_time_ws,
  upper(json_value(payload, "$.data.s")) as symbol,

  cast(json_value(payload, "$.data.U") as int64) as first_update_id,
  cast(json_value(payload, "$.data.u") as int64) as last_update_id,

  json_query(payload, "$.data.b") as bids,
  json_query(payload, "$.data.a") as asks

from {{ source('bronze', 'raw_orderbook') }}
