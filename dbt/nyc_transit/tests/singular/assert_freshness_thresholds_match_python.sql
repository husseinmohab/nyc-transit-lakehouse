/*
  The SLA thresholds in dq_pipeline_freshness.sql must match
  nyc_transit.quality.freshness.FRESHNESS_SLAS

  dbt cannot import Python so the thresholds are restated as SQL literals in
  the model Restating a number is fine restating it silently is not the
  expected values below are checked against the Python module by
  tests/unit/test_dbt_thresholds_match_python.py so a change to either side
  without the other fails twice
*/

with expected(table_name, max_lag_seconds_sla, severity) as (

    values
        ('silver.trip_stop_predictions', 420, 'critical'),
        ('silver.vehicle_positions', 420, 'critical'),
        ('silver.alert_informed_entities', 21600, 'warning')

)

select
    a.table_name,
    a.max_lag_seconds_sla as actual_sla,
    e.max_lag_seconds_sla as expected_sla,
    a.severity            as actual_severity,
    e.severity            as expected_severity
from {{ ref('dq_pipeline_freshness') }} a
inner join expected e on a.table_name = e.table_name
where a.max_lag_seconds_sla <> e.max_lag_seconds_sla
   or a.severity <> e.severity
