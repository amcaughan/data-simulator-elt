{{ config(
  partitioned_by=["event_date"],
  external_location=processed_table_location("bronze", "bronze_delivery_records")
) }}

with raw_rows as (
  select
    concat(cast(delivery_id as varchar), ':', cast(record_number as varchar)) as delivery_record_id,
    cast(source_system_id as varchar) as source_system_id,
    cast(delivery_id as varchar) as delivery_id,
    try(cast(delivery_date as date)) as delivery_date,
    cast(record_number as bigint) as record_number,
    cast(member_id as bigint) as member_id,
    cast(facility_id as varchar) as facility_id,
    cast(facility_region as varchar) as facility_region,
    cast(facility_type as varchar) as facility_type,
    cast(feed_type as varchar) as feed_type,
    cast(member_status as varchar) as member_status,
    cast(plan_tier as varchar) as plan_tier,
    cast(age_band as varchar) as age_band,
    cast(postal_prefix as varchar) as postal_prefix,
    cast(allowed_amount as double) as allowed_amount,
    cast(_landing_key as varchar) as landing_key,
    cast(_standardize_strategy as varchar) as standardize_strategy,
    cast(_source_preset_id as varchar) as source_preset_id,
    try(cast(from_iso8601_timestamp(_logical_date) as timestamp)) as logical_ts,
    try(cast(from_iso8601_timestamp(_ingested_at) as timestamp)) as ingested_ts,
    cast(_raw_bundle_id as varchar) as raw_bundle_id,
    cast(_raw_bundle_key as varchar) as raw_bundle_key,
    cast(_raw_bundle_manifest_key as varchar) as raw_bundle_manifest_key,
    try(cast(from_iso8601_timestamp(_raw_standardized_at) as timestamp)) as raw_standardized_ts,
    cast(_raw_bundle_granularity as varchar) as raw_bundle_granularity,
    cast(_raw_input_object_count as bigint) as raw_input_object_count,
    cast(_raw_bundle_row_count as bigint) as raw_bundle_row_count,
    coalesce(
      try(cast(delivery_date as date)),
      cast(date(from_iso8601_timestamp(_logical_date)) as date),
      cast(date(from_iso8601_timestamp(_raw_standardized_at)) as date)
    ) as event_date
  from {{ source('raw', 'raw_sample_file_delivery_01') }}
),
canonical_bundle_rows as (
  select raw_rows.*
  from raw_rows
  inner join {{ ref('bronze_canonical_raw_bundles') }} canonical_bundles
    on raw_rows.raw_bundle_id = canonical_bundles.raw_bundle_id
),
deduped as (
  select
    canonical_bundle_rows.*,
    row_number() over (
      partition by delivery_record_id
      order by ingested_ts desc nulls last, raw_standardized_ts desc nulls last, record_number desc
    ) as row_num
  from canonical_bundle_rows
)

select
  delivery_record_id,
  source_system_id,
  delivery_id,
  delivery_date,
  record_number,
  member_id,
  facility_id,
  facility_region,
  facility_type,
  feed_type,
  member_status,
  plan_tier,
  age_band,
  postal_prefix,
  allowed_amount,
  landing_key,
  standardize_strategy,
  source_preset_id,
  logical_ts,
  ingested_ts,
  raw_bundle_id,
  raw_bundle_key,
  raw_bundle_manifest_key,
  raw_standardized_ts,
  raw_bundle_granularity,
  raw_input_object_count,
  raw_bundle_row_count,
  event_date
from deduped
where row_num = 1
