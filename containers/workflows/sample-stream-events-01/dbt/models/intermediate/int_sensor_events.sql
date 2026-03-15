{{ config(location=processed_table_location('intermediate', 'int_sensor_events')) }}

with ranked as (
  select
    *,
    row_number() over (
      partition by emitter_event_id
      order by emitted_at desc, emission_index desc
    ) as emitter_event_rank
  from {{ ref('stg_sensor_events') }}
)
select
  workflow_name,
  source_preset_id,
  source_schema_version,
  source_scenario_name,
  source_seed,
  emitter_event_id,
  emission_batch_started_at,
  emitted_at,
  event_date,
  emission_index,
  device_id,
  site_id,
  device_type,
  temperature_c,
  pressure_kpa,
  device_status
from ranked
where emitter_event_rank = 1
