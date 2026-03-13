{{ config(partitioned_by=["event_date"]) }}

select
  t.event_date,
  t.channel,
  t.card_region,
  t.merchant_category,
  sum(case when coalesce(a.is_answer_key_anomaly, false) then 1 else 0 end) as anomaly_transaction_count,
  cast(sum(case when coalesce(a.is_answer_key_anomaly, false) then t.amount else 0 end) as double) as anomalous_amount,
  cast(sum(case when coalesce(a.is_answer_key_anomaly, false) then 1 else 0 end) as double) / nullif(count(*), 0) as anomaly_rate,
  cast(avg(case when coalesce(a.is_answer_key_anomaly, false) then t.amount end) as double) as average_anomalous_amount
from {{ ref('gold_transactions') }} as t
left join {{ ref('gold_transaction_answer_keys') }} as a
  on t.transaction_id = a.transaction_id
group by 1, 2, 3, 4
