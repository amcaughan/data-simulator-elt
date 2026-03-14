{{ config(
  partitioned_by=["delivery_date"],
  external_location=processed_table_location("silver", "silver_delivery_batches")
) }}

select
  location_code,
  location_label,
  source_file_name,
  delivery_id,
  feed_type,
  count(*) as record_count,
  cast(sum(allowed_amount) as double) as total_allowed_amount,
  cast(avg(allowed_amount) as double) as average_allowed_amount,
  delivery_date
from {{ ref('silver_delivery_records') }}
group by
  location_code,
  location_label,
  source_file_name,
  delivery_id,
  feed_type,
  delivery_date
