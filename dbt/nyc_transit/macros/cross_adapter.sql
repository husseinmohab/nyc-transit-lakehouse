{#
  Dialect shims

  The whole model tree compiles against both DuckDB (local and CI) and Spark
  (production Iceberg) Rather than branching inside every model the handful of
  genuinely divergent expressions live here the list is short on purpose each
  macro is a place the two targets could behave differently
#}

{% macro parent_station(column) %}
  {#- GTFS-RT stop ids are platform-level ("127N"); stops.txt is keyed on the
      parent station ("127") Strip a trailing N or S only because a few MTA
      stop ids have no direction suffix and truncating those breaks the join. -#}
  case
      when right({{ column }}, 1) in ('N', 'S') then left({{ column }}, length({{ column }}) - 1)
      else {{ column }}
  end
{% endmacro %}


{% macro platform_direction(column) %}
  case
      when right({{ column }}, 1) = 'N' then 'NORTH'
      when right({{ column }}, 1) = 'S' then 'SOUTH'
      else null
  end
{% endmacro %}


{% macro to_boolean(column) %}
  {#- Seeds arrive as the strings 'true'/'false' under some adapter and seed-type
      combinations and as native booleans under others this normalises both
      without a per-target seed file. -#}
  case
      when cast({{ column }} as varchar) in ('true', 'True', '1', 't') then true
      when cast({{ column }} as varchar) in ('false', 'False', '0', 'f') then false
      else null
  end
{% endmacro %}


{% macro datediff_seconds(start_column, end_column) %}
  {%- if target.type == 'duckdb' -%}
    date_diff('second', {{ start_column }}, {{ end_column }})
  {%- else -%}
    unix_timestamp({{ end_column }}) - unix_timestamp({{ start_column }})
  {%- endif -%}
{% endmacro %}


{% macro epoch_seconds(column) %}
  {%- if target.type == 'duckdb' -%}
    cast(epoch({{ column }}) as bigint)
  {%- else -%}
    unix_timestamp({{ column }})
  {%- endif -%}
{% endmacro %}


{% macro date_sub_days(date_expression, days) %}
  {%- if target.type == 'duckdb' -%}
    ({{ date_expression }}) - {{ days }}
  {%- else -%}
    date_sub({{ date_expression }}, {{ days }})
  {%- endif -%}
{% endmacro %}


{% macro dbt_date_part_hour(column) %}
  {%- if target.type == 'duckdb' -%}
    cast(date_part('hour', {{ column }}) as integer)
  {%- else -%}
    cast(hour({{ column }}) as integer)
  {%- endif -%}
{% endmacro %}


{% macro far_future_timestamp() %}
  {#- SCD2 open-interval sentinel. A sentinel rather than NULL, so that
      `where valid_from <= t and t < valid_to` works without a coalesce in every
      query and a range join cannot silently drop the current row. -#}
  cast('9999-12-31 23:59:59' as timestamp)
{% endmacro %}


{% macro incremental_window_predicate(date_column) %}
  {#- The partition window an incremental run reprocesses.

      With `--vars '{run_date: "2026-11-01"}'` (how Airflow invokes it) exactly
      that service date so a DAG run is partition-scoped and re-running it is
      idempotent

      Without run_date a lookback window off the table's own max date, for
      ad-hoc runs the lookback exists because the MTA republishes predictions
      for minutes after a trip ends and a late Kafka batch can land after
      midnight, so a strict `> max(date)` would permanently drop those rows. -#}
  {%- if var('run_date', '') != '' -%}
    {{ date_column }} = cast('{{ var("run_date") }}' as date)
  {%- else -%}
    {{ date_column }} >= (
        select {{ date_sub_days(
                    "coalesce(max(" ~ date_column ~ "), cast('1970-01-01' as date))",
                    var('incremental_lookback_days', 2)
                  ) }}
        from {{ this }}
    )
  {%- endif -%}
{% endmacro %}
