{{ config(
  partitioned_by=["bundle_event_date"],
  external_location=processed_table_location("bronze", "bronze_canonical_raw_bundles")
) }}

with bundle_candidates as (
  select distinct
    cast(_raw_bundle_id as varchar) as raw_bundle_id,
    cast(_raw_bundle_key as varchar) as raw_bundle_key,
    try(cast(from_iso8601_timestamp(_raw_standardized_at) as timestamp)) as raw_standardized_ts,
    try(cast(from_iso8601_timestamp(_raw_bundle_logical_date) as timestamp)) as bundle_logical_ts,
    cast(_raw_bundle_granularity as varchar) as bundle_granularity,
    coalesce(
      cast(_raw_bundle_logical_date as varchar),
      cast(_raw_bundle_id as varchar)
    ) as bundle_selector_key,
    cast(_raw_input_object_count as bigint) as raw_input_object_count,
    cast(_raw_bundle_row_count as bigint) as raw_bundle_row_count,
    coalesce(
      try(cast(delivery_date as date)),
      cast(date(from_iso8601_timestamp(_raw_bundle_logical_date)) as date),
      cast(date(from_iso8601_timestamp(_raw_standardized_at)) as date)
    ) as bundle_event_date
  from {{ source('raw', 'raw_sample_file_delivery_01') }}
),
bundle_rollup as (
  select
    raw_bundle_id,
    max(raw_standardized_ts) as raw_standardized_ts,
    max(bundle_logical_ts) as bundle_logical_ts,
    max(bundle_granularity) as bundle_granularity,
    max(bundle_selector_key) as bundle_selector_key,
    max(raw_input_object_count) as raw_input_object_count,
    sum(raw_bundle_row_count) as raw_bundle_row_count,
    count(*) as raw_bundle_file_count,
    max(bundle_event_date) as bundle_event_date
  from bundle_candidates
  group by raw_bundle_id
),
ranked as (
  select
    *,
    {{ canonical_bundle_rank(
      partition_by=["bundle_granularity", "bundle_selector_key"],
      order_by=["raw_standardized_ts desc nulls last", "raw_bundle_id desc"]
    ) }} as canonical_bundle_rank
  from bundle_rollup
)

select
  raw_bundle_id,
  raw_standardized_ts,
  bundle_logical_ts,
  bundle_granularity,
  bundle_selector_key,
  raw_input_object_count,
  raw_bundle_row_count,
  raw_bundle_file_count,
  bundle_event_date
from ranked
where canonical_bundle_rank = 1
