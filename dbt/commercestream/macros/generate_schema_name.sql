{% macro generate_schema_name(custom_schema_name, node) -%}
  {#- Use custom schema as-is (STAGING / MARTS) instead of target_schema_custom. -#}
  {%- if custom_schema_name is none -%}
    {{ target.schema }}
  {%- else -%}
    {{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
