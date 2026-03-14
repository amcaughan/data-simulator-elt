{{ config(
  partitioned_by=["event_date"],
  external_location=processed_table_location("silver", "silver_delivery_records")
) }}

select
  delivery_record_id,
  source_system_id as location_code,
  case
    when source_system_id = 'location_1' then 'Location 1'
    when source_system_id = 'location_2' then 'Location 2'
    else source_system_id
  end as location_label,
  delivery_id,
  delivery_date,
  regexp_extract(landing_key, '([^/]+\\.csv)$', 1) as source_file_name,
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
  raw_bundle_id,
  event_date
from {{ ref('bronze_delivery_records') }}
