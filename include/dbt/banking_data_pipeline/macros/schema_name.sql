{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {# Jika dbt run guna profile prod, dia tambah _prod (cth: bronze_prod) #}
        {%- if target.name == 'prod' -%}
            {{ custom_schema_name | trim }}_prod
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}