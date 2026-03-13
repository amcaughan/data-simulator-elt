{{ config(partitioned_by=["event_date"]) }}

select
  transaction_id,
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
  amount_band,
  channel,
  is_declined,
  is_high_risk_merchant
from {{ ref('gold_transactions') }}
