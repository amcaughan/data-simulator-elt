{{ config(location=marts_table_location('management', 'management_stream_health_summary')) }}

select
  event_date,
  count(*) as event_count,
  count(distinct device_id) as distinct_device_count,
  count(distinct site_id) as distinct_site_count,
  sum(case when device_status = 'warning' then 1 else 0 end) as warning_event_count,
  sum(case when pressure_kpa is null then 1 else 0 end) as missing_pressure_event_count
from {{ ref('int_sensor_events') }}
group by 1
