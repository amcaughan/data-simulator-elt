{{ config(partitioned_by=["event_date"]) }}

select
  t.transaction_id,
  t.event_ts,
  t.event_date,
  t.event_hour,
  t.card_region,
  t.card_segment,
  t.merchant_category,
  t.merchant_region,
  t.merchant_risk_tier,
  t.amount,
  t.amount_band,
  t.channel,
  t.is_declined,
  coalesce(a.is_answer_key_anomaly, false) as is_answer_key_anomaly,
  coalesce(a.answer_key_label_count, 0) as answer_key_label_count,
  a.answer_key_labels_json
from {{ ref('gold_transactions') }} as t
left join {{ ref('gold_transaction_answer_keys') }} as a
  on t.transaction_id = a.transaction_id
