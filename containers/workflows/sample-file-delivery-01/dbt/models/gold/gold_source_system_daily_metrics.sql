{{ config(
  partitioned_by=["event_date"],
  external_location=processed_table_location("gold", "gold_source_system_daily_metrics")
) }}

select
  location_code,
  location_label,
  count(*) as record_count,
  count(distinct delivery_id) as delivery_count,
  cast(sum(allowed_amount) as double) as total_allowed_amount,
  cast(avg(allowed_amount) as double) as average_allowed_amount,
  event_date
from {{ ref('silver_delivery_records') }}
group by event_date, location_code, location_label
