{#
  where a model lands

  dbt's default glues target.schema and the folder's +schema together so on
  duckdb staging models go to main_staging which is what the local target and
  every test expects

  against glue the schema IS the database and modules/glue_catalog creates one
  per layer with a prefix so the name has to be prefix_staging and not
  something_staging appended to whatever target.schema happens to be
  pass glue_db_prefix the value of the glue_database_prefix terraform output

  this applies to every engine that reads the glue catalog, athena and spark
  both, so the branch tests for duckdb
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if target.type == 'duckdb' -%}
    {{ default__generate_schema_name(custom_schema_name, node) }}
  {%- elif custom_schema_name is none -%}
    {{ target.schema | trim }}
  {%- else -%}
    {%- set prefix = var('glue_db_prefix', '') | trim -%}
    {%- set layer = custom_schema_name | trim -%}
    {%- if prefix -%}
      {{ prefix ~ '_' ~ layer }}
    {%- else -%}
      {{ layer }}
    {%- endif -%}
  {%- endif -%}
{%- endmacro %}
