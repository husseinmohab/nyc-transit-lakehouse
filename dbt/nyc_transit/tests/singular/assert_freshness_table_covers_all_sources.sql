/*
  dq_pipeline_freshness must have a row for every silver source this project
  declares A table that quietly stops being monitored is worse than one that
  was never monitored because the dashboard still looks green
*/

with expected as (

    select 'silver.trip_stop_predictions' as table_name
    union all select 'silver.vehicle_positions'
    union all select 'silver.alert_informed_entities'

),

actual as (

    select table_name from {{ ref('dq_pipeline_freshness') }}

)

select
    e.table_name,
    'missing_from_freshness_table' as problem
from expected e
left join actual a on e.table_name = a.table_name
where a.table_name is null
