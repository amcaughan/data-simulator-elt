{{ config(
  partitioned_by=["event_date"],
  external_location=marts_table_location("analytics", "analytics_daily_delivery_metrics")
) }}

select *
from {{ ref('gold_daily_delivery_metrics') }}
