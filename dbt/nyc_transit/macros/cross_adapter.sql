{#
  Dialect shims

  the model tree compiles against three engines
  duckdb for local and CI, athena for production, spark for a cluster that
  already exists

  athena is trino dialect and is the odd one out
  it has no left() or right(), it rejects `cast(x as string)` and wants varchar,
  and it spells epoch conversion and date arithmetic differently

  the divergent expressions live here so no model has to branch
  the list is short on purpose, each macro is a place the engines differ
#}

{% macro is_trino() %}
  {#- athena engine v3 is trino -#}
  {{ return(target.type == 'athena') }}
{% endmacro %}


{% macro as_text(column) %}
  {#- the text type, which is the one cast every engine spells differently
      spark rejects a bare varchar with DATATYPE_MISSING_SIZE and wants
      varchar(n), trino has no `string` type at all -#}
  {%- if is_trino() -%}
    cast({{ column }} as varchar)
  {%- else -%}
    cast({{ column }} as string)
  {%- endif -%}
{% endmacro %}

{% macro now_timestamp() %}
  {#- the current instant as a timestamp WITHOUT a zone
      trino's current_timestamp is `timestamp with time zone` and a hive table
      cannot hold that, the create fails with
      "Unsupported Hive type: timestamp(3) with time zone"
      localtimestamp is the zoneless one -#}
  {%- if is_trino() -%}
    localtimestamp
  {%- else -%}
    current_timestamp
  {%- endif -%}
{% endmacro %}


{% macro hash_text(expression) %}
  {#- a stable hex digest of a text expression
      trino's md5 takes varbinary and returns varbinary so it needs the utf8
      encode and the hex decode around it -#}
  {%- if is_trino() -%}
    lower(to_hex(md5(to_utf8({{ expression }}))))
  {%- else -%}
    md5({{ expression }})
  {%- endif -%}
{% endmacro %}


{% macro parent_station(column) %}
  {#- GTFS-RT stop ids are platform-level ("127N"); stops.txt is keyed on the
      parent station ("127") Strip a trailing N or S only because a few MTA
      stop ids have no direction suffix and truncating those breaks the join. -#}
  {%- if is_trino() -%}
  case
      when substr({{ column }}, -1) in ('N', 'S')
        then substr({{ column }}, 1, length({{ column }}) - 1)
      else {{ column }}
  end
  {%- else -%}
  case
      when right({{ column }}, 1) in ('N', 'S') then left({{ column }}, length({{ column }}) - 1)
      else {{ column }}
  end
  {%- endif -%}
{% endmacro %}


{% macro platform_direction(column) %}
  {%- if is_trino() -%}
  case
      when substr({{ column }}, -1) = 'N' then 'NORTH'
      when substr({{ column }}, -1) = 'S' then 'SOUTH'
      else null
  end
  {%- else -%}
  case
      when right({{ column }}, 1) = 'N' then 'NORTH'
      when right({{ column }}, 1) = 'S' then 'SOUTH'
      else null
  end
  {%- endif -%}
{% endmacro %}


{% macro to_boolean(column) %}
  {#- Seeds arrive as the strings 'true'/'false' under some adapter and seed-type
      combinations and as native booleans under others this normalises both
      without a per-target seed file. -#}
  case
      when {{ as_text(column) }} in ('true', 'True', '1', 't') then true
      when {{ as_text(column) }} in ('false', 'False', '0', 'f') then false
      else null
  end
{% endmacro %}


{% macro datediff_seconds(start_column, end_column) %}
  {%- if target.type == 'duckdb' or is_trino() -%}
    date_diff('second', {{ start_column }}, {{ end_column }})
  {%- else -%}
    unix_timestamp({{ end_column }}) - unix_timestamp({{ start_column }})
  {%- endif -%}
{% endmacro %}


{% macro epoch_seconds(column) %}
  {%- if target.type == 'duckdb' -%}
    cast(epoch({{ column }}) as bigint)
  {%- elif is_trino() -%}
    cast(to_unixtime({{ column }}) as bigint)
  {%- else -%}
    unix_timestamp({{ column }})
  {%- endif -%}
{% endmacro %}


{% macro date_sub_days(date_expression, days) %}
  {%- if target.type == 'duckdb' -%}
    ({{ date_expression }}) - {{ days }}
  {%- elif is_trino() -%}
    date_add('day', -{{ days }}, {{ date_expression }})
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
