{{ config(
  partitioned_by=["event_date"],
  external_location=processed_table_location("bronze", "bronze_transaction_events")
) }}

with raw_rows as (
  select
    concat(_landing_key, ':', cast(__row_index as varchar)) as transaction_id,
    cast(__row_index as bigint) as source_row_index,
    cast(from_iso8601_timestamp(_logical_date) as timestamp) as event_ts,
    cast(date_trunc('hour', from_iso8601_timestamp(_logical_date)) as timestamp) as event_hour,
    cast(card_id as varchar) as card_id,
    cast(card_region as varchar) as card_region,
    cast(card_segment as varchar) as card_segment,
    cast(merchant_id as varchar) as merchant_id,
    cast(merchant_category as varchar) as merchant_category,
    cast(merchant_region as varchar) as merchant_region,
    cast(merchant_risk_tier as varchar) as merchant_risk_tier,
    cast(amount as double) as amount,
    cast(channel as varchar) as channel,
    case when cast(is_declined as integer) = 1 then true else false end as is_declined,
    cast(__is_anomaly as boolean) as is_answer_key_anomaly,
    cast(__labels as varchar) as answer_key_labels_json,
    cardinality(
      coalesce(
        try(cast(json_parse(__labels) as array(json))),
        cast(array[] as array(json))
      )
    ) as answer_key_label_count,
    cast(_landing_key as varchar) as landing_key,
    cast(_standardize_strategy as varchar) as standardize_strategy,
    cast(_source_preset_id as varchar) as source_preset_id,
    try(cast(from_iso8601_timestamp(_ingested_at) as timestamp)) as ingested_ts,
    cast(_schema_version as varchar) as schema_version,
    cast(_scenario_name as varchar) as scenario_name,
    cast(_response_row_count as bigint) as response_row_count,
    cast(_raw_bundle_id as varchar) as raw_bundle_id,
    cast(_raw_bundle_key as varchar) as raw_bundle_key,
    cast(_raw_bundle_manifest_key as varchar) as raw_bundle_manifest_key,
    try(cast(from_iso8601_timestamp(_raw_standardized_at) as timestamp)) as raw_standardized_ts,
    cast(_raw_bundle_granularity as varchar) as raw_bundle_granularity,
    cast(_raw_input_object_count as bigint) as raw_input_object_count,
    cast(_raw_bundle_row_count as bigint) as raw_bundle_row_count,
    cast(date(from_iso8601_timestamp(_logical_date)) as date) as event_date
  from {{ source('raw', 'raw_polling_generated_events') }}
  where regexp_like("$path", '.*\\.parquet$')
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
      partition by transaction_id
      order by ingested_ts desc nulls last, event_ts desc, source_row_index desc
    ) as row_num
  from canonical_bundle_rows
)

select
  transaction_id,
  source_row_index,
  event_ts,
  event_hour,
  card_id,
  card_region,
  card_segment,
  merchant_id,
  merchant_category,
  merchant_region,
  merchant_risk_tier,
  amount,
  channel,
  is_declined,
  is_answer_key_anomaly,
  answer_key_labels_json,
  answer_key_label_count,
  landing_key,
  standardize_strategy,
  source_preset_id,
  ingested_ts,
  schema_version,
  scenario_name,
  response_row_count,
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
