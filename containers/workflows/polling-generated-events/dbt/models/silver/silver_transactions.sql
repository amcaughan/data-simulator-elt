{{ config(partitioned_by=["event_date"]) }}

select
  concat(_landing_key, ':', cast(__row_index as varchar)) as transaction_id,
  cast(__row_index as bigint) as source_row_index,
  cast(from_iso8601_timestamp(_logical_date) as timestamp) as event_ts,
  cast(date_trunc('hour', from_iso8601_timestamp(_logical_date)) as timestamp) as event_hour,
  cast(card_id as varchar) as card_id,
  cast(card_region as varchar) as card_region,
  cast(card_segment as varchar) as card_segment,
  cast(merchant_id as varchar) as merchant_id,
  cast(merchant_category as varchar) as merchant_category,
  cast(merchant_region as varchar) as merchant_region,
  cast(merchant_risk_tier as varchar) as merchant_risk_tier,
  cast(amount as double) as amount,
  cast(channel as varchar) as channel,
  case when cast(is_declined as integer) = 1 then true else false end as is_declined,
  cast(_landing_key as varchar) as landing_key,
  cast(_standardize_strategy as varchar) as standardize_strategy,
  cast(_source_preset_id as varchar) as source_preset_id,
  try(cast(from_iso8601_timestamp(_ingested_at) as timestamp)) as ingested_ts,
  cast(_schema_version as varchar) as schema_version,
  cast(_scenario_name as varchar) as scenario_name,
  cast(_response_row_count as bigint) as response_row_count,
  cast(date(from_iso8601_timestamp(_logical_date)) as date) as event_date
from {{ source('bronze', 'bronze_polling_generated_events') }}
