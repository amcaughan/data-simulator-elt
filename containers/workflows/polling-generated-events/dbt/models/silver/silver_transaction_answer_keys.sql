{{ config(
  partitioned_by=["event_date"],
  external_location=processed_table_location("silver", "silver_transaction_answer_keys")
) }}

select
  transaction_id,
  event_ts,
  event_hour,
  is_answer_key_anomaly,
  answer_key_labels_json,
  answer_key_label_count,
  landing_key,
  event_date
from {{ ref('bronze_transaction_events') }}
where is_answer_key_anomaly or answer_key_label_count > 0
