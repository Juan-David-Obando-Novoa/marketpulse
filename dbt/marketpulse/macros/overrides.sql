{#
    Schema naming.

    dbt's default concatenates the profile schema with the model's custom
    schema, producing `silver_gold`. The lake's namespaces are fixed by the
    Iceberg DDL, so the custom schema wins outright and the profile schema is
    only a fallback.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}


{#  A run summary in the logs, so a scheduled run leaves a readable trace
    rather than requiring someone to open the artifacts.  #}
{% macro log_run_summary() %}
    {% if execute and results %}
        {% set failed = results | selectattr('status', 'in', ['error', 'fail']) | list %}
        {% set warned = results | selectattr('status', 'equalto', 'warn') | list %}
        {{ log(
            'dbt run summary: ' ~ results | length ~ ' nodes, '
            ~ failed | length ~ ' failed, ' ~ warned | length ~ ' warned',
            info=true
        ) }}
        {% for result in failed %}
            {{ log('  FAILED ' ~ result.node.unique_id ~ ': ' ~ result.message, info=true) }}
        {% endfor %}
    {% endif %}
{% endmacro %}


{#
    Iceberg table properties applied to every materialised model.

    Set through a macro rather than per model so that changing the compression
    codec or the target file size is one edit, and so the properties cannot
    drift between models the way copy-pasted config blocks do.
#}
{#
    DIALECT WARNING for `partitioning`: the bucket transform is spelled
    differently by the two engines writing this lakehouse, and both spellings
    are correct in their own place.

        Spark  (src/marketpulse/streaming/ddl/bronze.sql):  bucket(16, symbol)
        Trino  (everything under dbt/):                     bucket(symbol, 16)

    Same Iceberg transform, same resulting partition spec -- only the argument
    order differs. Trino rejects the Spark spelling with "Invalid partition
    field declaration", which reads like the transform is unsupported rather
    than merely written back to front. Do not "fix" the bronze DDL to match
    these models, or vice versa.
#}
{% macro iceberg_properties(partitioning=none, sorted_by=none, target_file_size='134217728') %}
    {%- set props = {
        'format': "'PARQUET'",
        'format_version': '2'
    } -%}
    {%- if partitioning -%}
        {%- do props.update({'partitioning': "ARRAY" ~ partitioning}) -%}
    {%- endif -%}
    {%- if sorted_by -%}
        {%- do props.update({'sorted_by': "ARRAY" ~ sorted_by}) -%}
    {%- endif -%}
    {{ return(props) }}
{% endmacro %}
