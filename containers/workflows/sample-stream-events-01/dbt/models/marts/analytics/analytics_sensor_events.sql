{{ config(
  partitioned_by=["event_date"],
  external_location=marts_table_location("analytics", "analytics_sensor_events")
) }}

select
  workflow_name,
  source_preset_id,
  source_scenario_name,
  emitter_event_id,
  emitted_at,
  device_id,
  site_id,
  device_type,
  temperature_c,
  pressure_kpa,
  device_status,
  event_date
from {{ ref('int_sensor_events') }}
