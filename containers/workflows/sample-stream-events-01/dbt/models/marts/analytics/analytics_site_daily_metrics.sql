{{ config(
  partitioned_by=["event_date"],
  external_location=marts_table_location("analytics", "analytics_site_daily_metrics")
) }}

select
  site_id,
  device_type,
  count(*) as event_count,
  count(distinct device_id) as device_count,
  avg(temperature_c) as avg_temperature_c,
  avg(pressure_kpa) as avg_pressure_kpa,
  sum(case when device_status = 'warning' then 1 else 0 end) as warning_event_count,
  sum(case when pressure_kpa is null then 1 else 0 end) as missing_pressure_event_count,
  event_date
from {{ ref('int_sensor_events') }}
group by site_id, device_type, event_date
