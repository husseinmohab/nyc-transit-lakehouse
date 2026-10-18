{#
  SCD Type 2 helpers

  Used by models/marts/core/dim_vehicle_assignment.sql Factored out so the
  interval-closing logic is written once and a second SCD2 dimension can reuse
  it without copy-paste drift
#}

{% macro scd2_surrogate_key(business_key_columns, valid_from_column) %}
  {#- Surrogate key for an SCD2 row: the business key plus the instant the
      version opened hashed rather than sequenced because a sequence is not
      stable across a full refresh and a dimension whose keys change on rebuild
      cannot be referenced by a fact that was not rebuilt with it. -#}
  md5(
    {%- for column in business_key_columns %}
      coalesce(cast({{ column }} as varchar), '~') || '|' ||
    {%- endfor %}
      coalesce(cast({{ valid_from_column }} as varchar), '~')
  )
{% endmacro %}


{% macro scd2_change_detected(tracked_columns, previous_alias='prev') %}
  {#- True when any tracked attribute differs from the previous version.

      Each comparison is NULL-safe `a <> b` is NULL when either side is NULL,
      so a plain inequality would miss a transition into or out of NULL which
      for this dimension is the assigned/unassigned transition that matters
      most. -#}
  (
  {%- for column in tracked_columns %}
      coalesce(cast({{ column }} as varchar), '~#NULL#~')
        <> coalesce(cast({{ previous_alias }}_{{ column }} as varchar), '~#NULL#~')
      {%- if not loop.last %} or {% endif %}
  {%- endfor %}
  )
{% endmacro %}
