{{ config(partitioned_by=["event_date"]) }}

select
  t.event_date,
  t.channel,
  t.card_region,
  t.merchant_category,
  t.transaction_count,
  t.gross_amount,
  t.average_amount,
  t.declined_transaction_count,
  t.decline_rate,
  coalesce(a.anomaly_transaction_count, 0) as anomaly_transaction_count,
  coalesce(a.anomalous_amount, 0.0) as anomalous_amount,
  coalesce(a.anomaly_rate, 0.0) as anomaly_rate,
  a.average_anomalous_amount
from {{ ref('gold_daily_transaction_metrics') }} as t
left join {{ ref('gold_daily_anomaly_metrics') }} as a
  on t.event_date = a.event_date
 and t.channel = a.channel
 and t.card_region = a.card_region
 and t.merchant_category = a.merchant_category
