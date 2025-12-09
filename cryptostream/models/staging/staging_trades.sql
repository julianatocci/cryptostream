{{ config(materialized='view') }}

select
  lower(stream) as stream,
  ingest_ts,
  event_ts,

  upper(json_value(payload, "$.s")) as symbol,
  cast(json_value(payload, "$.t") as int64) as trade_id,
  cast(json_value(payload, "$.p") as numeric) as price,
  cast(json_value(payload, "$.q") as numeric) as qty,
  cast(json_value(payload, "$.m") as bool) as is_buyer_maker,

  timestamp_millis(cast(json_value(payload, "$.T") as int64)) as trade_ts,
  timestamp_millis(cast(json_value(payload, "$.E") as int64)) as event_time_ws

from {{ source('bronze','raw_trades') }}
