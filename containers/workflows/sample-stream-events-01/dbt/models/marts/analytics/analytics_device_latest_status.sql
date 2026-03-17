{{ config(
  partitioned_by=["event_date"],
  external_location=marts_table_location("analytics", "analytics_device_latest_status")
) }}

with ranked as (
  select
    *,
    row_number() over (
      partition by device_id
      order by emitted_at desc, emission_index desc
    ) as device_rank
  from {{ ref('int_sensor_events') }}
)
select
  device_id,
  site_id,
  device_type,
  device_status,
  temperature_c,
  pressure_kpa,
  emitted_at,
  event_date
from ranked
where device_rank = 1
