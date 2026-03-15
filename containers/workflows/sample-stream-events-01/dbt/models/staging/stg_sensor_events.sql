{{ config(location=processed_table_location('staging', 'stg_sensor_events')) }}

with source_events as (
  select *
  from {{ source('stream_raw', 'raw_sample_stream_events_01') }}
),
typed as (
  select
    cast(workflow_name as varchar) as workflow_name,
    cast(source_preset_id as varchar) as source_preset_id,
    cast(source_schema_version as varchar) as source_schema_version,
    cast(source_scenario_name as varchar) as source_scenario_name,
    cast(source_seed as bigint) as source_seed,
    cast(emitter_event_id as varchar) as emitter_event_id,
    cast(from_iso8601_timestamp(emission_batch_started_at) as timestamp) as emission_batch_started_at,
    cast(from_iso8601_timestamp(emitted_at) as timestamp) as emitted_at,
    cast(date(from_iso8601_timestamp(emitted_at)) as date) as event_date,
    cast(emission_index as integer) as emission_index,
    cast(device_id as varchar) as device_id,
    cast(site_id as varchar) as site_id,
    cast(device_type as varchar) as device_type,
    cast(temperature_c as double) as temperature_c,
    cast(pressure_kpa as double) as pressure_kpa,
    cast(device_status as varchar) as device_status
  from source_events
)
select *
from typed
where emitter_event_id is not null
