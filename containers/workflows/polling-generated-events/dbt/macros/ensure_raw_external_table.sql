{% macro ensure_raw_external_table() %}
  {% if execute %}
    {% set raw_table = target.schema ~ '.raw_polling_generated_events' %}
    {% set raw_location = "s3://" ~ env_var("PROCESSED_BUCKET_NAME") ~ "/raw/" %}
    {% set create_sql %}
      CREATE EXTERNAL TABLE IF NOT EXISTS {{ raw_table }} (
        `__row_index` bigint,
        `__is_anomaly` boolean,
        `__labels` string,
        `card_id` string,
        `card_region` string,
        `card_segment` string,
        `merchant_id` string,
        `merchant_category` string,
        `merchant_region` string,
        `merchant_risk_tier` string,
        `amount` double,
        `channel` string,
        `is_declined` bigint,
        `_landing_key` string,
        `_standardize_strategy` string,
        `_source_preset_id` string,
        `_logical_date` string,
        `_ingested_at` string,
        `_schema_version` string,
        `_scenario_name` string,
        `_response_row_count` bigint
      )
      PARTITIONED BY (
        `year` string,
        `month` string,
        `day` string
      )
      STORED AS PARQUET
      LOCATION '{{ raw_location }}'
    {% endset %}
    {% do run_query(create_sql) %}
    {% do run_query("MSCK REPAIR TABLE " ~ raw_table) %}
  {% endif %}
{% endmacro %}
