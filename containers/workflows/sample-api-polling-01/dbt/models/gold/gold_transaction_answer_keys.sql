{{ config(
  partitioned_by=["event_date"],
  external_location=processed_table_location("gold", "gold_transaction_answer_keys")
) }}

select
  transaction_id,
  event_ts,
  event_hour,
  is_answer_key_anomaly,
  answer_key_label_count,
  answer_key_labels_json,
  case when answer_key_label_count > 0 then true else false end as has_answer_key_labels,
  landing_key,
  event_date
from {{ ref('silver_transaction_answer_keys') }}
