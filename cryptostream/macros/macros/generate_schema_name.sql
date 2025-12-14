{% macro generate_schema_name(custom_schema_name, node) -%}
  {#
    BigQuery: queremos usar exatamente o schema definido no model/project,
    sem prefixar com target.dataset
  #}
  {{ custom_schema_name if custom_schema_name is not none else target.dataset }}
{%- endmacro %}
