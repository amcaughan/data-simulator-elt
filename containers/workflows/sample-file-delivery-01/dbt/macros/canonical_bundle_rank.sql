{% macro canonical_bundle_rank(partition_by, order_by) -%}
row_number() over (
  partition by {{ partition_by | join(", ") }}
  order by {{ order_by | join(", ") }}
)
{%- endmacro %}
