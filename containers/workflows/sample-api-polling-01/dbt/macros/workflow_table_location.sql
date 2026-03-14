{% macro processed_table_location(layer_name, table_name) -%}
  {{ return("s3://" ~ env_var("PROCESSED_BUCKET_NAME") ~ "/" ~ layer_name ~ "/" ~ table_name ~ "/") }}
{%- endmacro %}

{% macro marts_table_location(layer_name, table_name) -%}
  {{ return("s3://" ~ env_var("MARTS_BUCKET_NAME") ~ "/" ~ layer_name ~ "/" ~ table_name ~ "/") }}
{%- endmacro %}
