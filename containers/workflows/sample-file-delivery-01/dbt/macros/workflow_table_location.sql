{% macro processed_table_location(layer_name, table_name) -%}
  {{ return(env_var("PROCESS_S3_ROOT") ~ layer_name ~ "/" ~ table_name ~ "/") }}
{%- endmacro %}

{% macro marts_table_location(layer_name, table_name) -%}
  {{ return(env_var("SURFACE_S3_ROOT") ~ layer_name ~ "/" ~ table_name ~ "/") }}
{%- endmacro %}
