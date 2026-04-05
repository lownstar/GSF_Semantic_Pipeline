-- Override dbt's default schema naming so models land in the correct Snowflake
-- schemas (SILVER, GOLD_NAIVE, GOLD) rather than <default_schema>_<custom_schema>.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | upper | trim }}
    {%- endif -%}
{%- endmacro %}
