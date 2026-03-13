{{ config(partitioned_by=["event_date"]) }}

select
  transaction_id,
  source_row_index,
  event_ts,
  event_date,
  event_hour,
  card_id,
  card_region,
  card_segment,
  merchant_id,
  merchant_category,
  merchant_region,
  merchant_risk_tier,
  amount,
  channel,
  is_declined,
  case
    when amount < 25 then 'micro'
    when amount < 100 then 'small'
    when amount < 250 then 'medium'
    else 'large'
  end as amount_band,
  case
    when lower(merchant_risk_tier) in ('high', 'critical') then true
    else false
  end as is_high_risk_merchant,
  cast(date_trunc('day', event_ts) as timestamp) as event_day_ts,
  source_preset_id,
  ingested_ts,
  schema_version,
  scenario_name,
  response_row_count,
  landing_key
from {{ ref('silver_transactions') }}
