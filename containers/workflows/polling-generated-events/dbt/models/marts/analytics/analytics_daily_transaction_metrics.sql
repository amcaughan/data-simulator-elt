{{ config(partitioned_by=["event_date"]) }}

select
  event_date,
  channel,
  card_region,
  merchant_category,
  transaction_count,
  gross_amount,
  average_amount,
  declined_transaction_count,
  declined_amount,
  decline_rate
from {{ ref('gold_daily_transaction_metrics') }}
