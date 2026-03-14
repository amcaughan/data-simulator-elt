{{ config(
  partitioned_by=["event_date"],
  external_location=processed_table_location("silver", "silver_transactions")
) }}

select
  transaction_id,
  source_row_index,
  event_ts,
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
  landing_key,
  standardize_strategy,
  source_preset_id,
  ingested_ts,
  schema_version,
  scenario_name,
  response_row_count,
  event_date
from {{ ref('bronze_transaction_events') }}
