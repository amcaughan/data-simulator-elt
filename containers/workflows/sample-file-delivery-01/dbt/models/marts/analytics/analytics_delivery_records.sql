{{ config(
  partitioned_by=["event_date"],
  external_location=marts_table_location("analytics", "analytics_delivery_records")
) }}

select *
from {{ ref('silver_delivery_records') }}
