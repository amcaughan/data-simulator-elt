{{ config(partitioned_by=["event_date"]) }}

with parsed as (
  select
    concat(_landing_key, ':', cast(__row_index as varchar)) as transaction_id,
    cast(from_iso8601_timestamp(_logical_date) as timestamp) as event_ts,
    cast(date(from_iso8601_timestamp(_logical_date)) as date) as event_date,
    cast(date_trunc('hour', from_iso8601_timestamp(_logical_date)) as timestamp) as event_hour,
    cast(__is_anomaly as boolean) as is_answer_key_anomaly,
    cast(__labels as varchar) as answer_key_labels_json,
    cardinality(
      coalesce(
        try(cast(json_parse(__labels) as array(json))),
        cast(array[] as array(json))
      )
    ) as answer_key_label_count,
    cast(_landing_key as varchar) as landing_key
  from {{ source('bronze', 'bronze_polling_generated_events') }}
)

select
  transaction_id,
  event_ts,
  event_date,
  event_hour,
  is_answer_key_anomaly,
  answer_key_labels_json,
  answer_key_label_count,
  landing_key
from parsed
where is_answer_key_anomaly or answer_key_label_count > 0
