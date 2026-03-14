{{ config(
  partitioned_by=["event_date"],
  external_location=marts_table_location("management", "management_daily_summary")
) }}

select
  record_count,
  delivery_count,
  location_count,
  total_allowed_amount,
  average_allowed_amount,
  event_date
from {{ ref('gold_daily_delivery_metrics') }}
