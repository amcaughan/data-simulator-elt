{{ config(location=marts_table_location('analytics', 'analytics_sensor_events')) }}

select
  workflow_name,
  source_preset_id,
  source_scenario_name,
  emitter_event_id,
  emitted_at,
  event_date,
  device_id,
  site_id,
  device_type,
  temperature_c,
  pressure_kpa,
  device_status
from {{ ref('int_sensor_events') }}
