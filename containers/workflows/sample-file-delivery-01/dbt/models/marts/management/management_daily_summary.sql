{{ config(
  partitioned_by=["event_date"],
  external_location=marts_table_location("management", "management_daily_summary")
) }}

select
  event_date,
  record_count,
  delivery_count,
  location_count,
  total_allowed_amount,
  average_allowed_amount
from {{ ref('gold_daily_delivery_metrics') }}
