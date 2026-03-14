{% macro ensure_raw_external_table() %}
  {% if execute %}
    {% set raw_table = target.schema ~ '.raw_sample_file_delivery_01' %}
    {% set raw_location = "s3://" ~ env_var("PROCESSED_BUCKET_NAME") ~ "/raw/" %}
    {% set create_sql %}
      CREATE EXTERNAL TABLE IF NOT EXISTS {{ raw_table }} (
        `source_system_id` string,
        `delivery_id` string,
        `delivery_date` string,
        `record_number` string,
        `member_id` string,
        `facility_id` string,
        `facility_region` string,
        `facility_type` string,
        `feed_type` string,
        `member_status` string,
        `plan_tier` string,
        `age_band` string,
        `postal_prefix` string,
        `allowed_amount` string,
        `_landing_key` string,
        `_standardize_strategy` string,
        `_source_preset_id` string,
        `_logical_date` string,
        `_ingested_at` string,
        `_source_system_id` string,
        `_delivery_id` string,
        `_delivery_date` string,
        `_feed_type` string,
        `_response_row_count` string,
        `_raw_bundle_id` string,
        `_raw_bundle_key` string,
        `_raw_bundle_manifest_key` string,
        `_raw_standardized_at` string,
        `_raw_bundle_logical_date` string,
        `_raw_bundle_granularity` string,
        `_raw_input_object_count` bigint,
        `_raw_bundle_row_count` bigint
      )
      PARTITIONED BY (
        `year` string,
        `month` string,
        `day` string
      )
      STORED AS PARQUET
      LOCATION '{{ raw_location }}'
    {% endset %}
    {% do run_query("DROP TABLE IF EXISTS " ~ raw_table) %}
    {% do run_query(create_sql) %}
    {% do run_query("MSCK REPAIR TABLE " ~ raw_table) %}
  {% endif %}
{% endmacro %}
