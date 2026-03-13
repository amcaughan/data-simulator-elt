{{ config(partitioned_by=["event_date"]) }}

select
  transaction_id,
  event_ts,
  event_date,
  event_hour,
  is_answer_key_anomaly,
  answer_key_label_count,
  answer_key_labels_json,
  case when answer_key_label_count > 0 then true else false end as has_answer_key_labels,
  landing_key
from {{ ref('silver_transaction_answer_keys') }}
