{% macro ensure_stream_external_table() %}
  {% if execute %}
    {% set stream_table = target.schema ~ '.raw_sample_stream_events_01' %}
    {% set stream_location = "s3://" ~ env_var("PROCESSED_BUCKET_NAME") ~ "/events/" %}
    {% set create_sql %}
      CREATE EXTERNAL TABLE IF NOT EXISTS {{ stream_table }} (
        `workflow_name` string,
        `source_preset_id` string,
        `source_schema_version` string,
        `source_scenario_name` string,
        `source_seed` bigint,
        `emitter_event_id` string,
        `emission_batch_started_at` string,
        `emitted_at` string,
        `emission_index` int,
        `device_id` string,
        `site_id` string,
        `device_type` string,
        `temperature_c` double,
        `pressure_kpa` double,
        `device_status` string
      )
      ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
      WITH SERDEPROPERTIES (
        'ignore.malformed.json'='true'
      )
      LOCATION '{{ stream_location }}'
      TBLPROPERTIES (
        'compressionType'='gzip'
      )
    {% endset %}
    {% do run_query("DROP TABLE IF EXISTS " ~ stream_table) %}
    {% do run_query(create_sql) %}
  {% endif %}
{% endmacro %}
