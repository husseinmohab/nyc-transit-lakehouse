{% snapshot snap_route_alert_status %}

{#
  file_format is declared here and not only in dbt_project.yml
  dbt-spark branches on target_relation.is_iceberg to decide where to put the
  snapshot's staging view since an iceberg catalog cannot hold a view at all
  without it the second run fails with "Replacing a view is not supported"
#}

{{
  config(
    target_schema = env_var('DBT_SNAPSHOT_SCHEMA', 'snapshots'),
    file_format = 'iceberg',
    unique_key = 'route_id',
    strategy = 'check',
    check_cols = ['service_status', 'alert_effect', 'alert_cause', 'open_alert_count'],
    invalidate_hard_deletes = True
  )
}}

/*
  scd2 history of each routes service status captured by dbt snapshot

  strategy check and not timestamp
  alert_published_at moves whenever any alert on the route is republished even
  with identical content so a timestamp strategy would open a version every run
  the check columns are the four attributes whose change means something

  check_cols leaves out header_text
  the mta rewords alerts constantly and snapshotting the prose would give a
  version per typo fix and bury the real transitions

  invalidate_hard_deletes so a route leaving the static feed closes its interval
  instead of leaving a dangling current row

  dbt keeps dbt_valid_from and dbt_valid_to
  assert_snapshot_has_one_current_row_per_route.sql asserts the invariant that
  matters which is exactly one open interval per route
*/

select
    route_id,
    route_short_name,
    service_status,
    alert_effect,
    alert_cause,
    open_alert_count,
    alert_entity_id,
    alert_published_at
from {{ ref('route_alert_status') }}

{% endsnapshot %}
