{{ config(materialized = 'table', tags = ['marts', 'quality']) }}

/*
  one row per monitored table with row count newest event and measured lag

  the dbt side mirror of nyc_transit.quality.probe
  the python probe runs in the airflow task and fails the dag while this table
  is queryable afterwards for when did freshness actually degrade
  if either goes missing the other makes it visible

  thresholds are restated here as literals since dbt cannot import python
  assert_freshness_thresholds_match_python.sql compares the two so a drift fails
*/

with observations as (

    select
        'silver.trip_stop_predictions' as table_name,
        count(*)                       as row_count,
        max(feed_timestamp)            as max_event_time,
        min(feed_timestamp)            as min_event_time,
        count(distinct service_date)   as partition_count,
        420                            as max_lag_seconds_sla,
        'critical'                     as severity
    from {{ ref('stg_trip_stop_predictions') }}

    union all

    select
        'silver.vehicle_positions'   as table_name,
        count(*)                     as row_count,
        max(reported_at)             as max_event_time,
        min(reported_at)             as min_event_time,
        count(distinct service_date) as partition_count,
        420                          as max_lag_seconds_sla,
        'critical'                   as severity
    from {{ ref('stg_vehicle_positions') }}

    union all

    select
        'silver.alert_informed_entities' as table_name,
        count(*)                         as row_count,
        max(feed_timestamp)              as max_event_time,
        min(feed_timestamp)              as min_event_time,
        count(distinct service_date)     as partition_count,
        21600                            as max_lag_seconds_sla,
        'warning'                        as severity
    from {{ ref('stg_alert_informed_entities') }}

)

select
    table_name,
    row_count,
    min_event_time,
    max_event_time,
    partition_count,
    max_lag_seconds_sla,
    severity,
    current_timestamp as measured_at,
    cast(
        {{ datediff_seconds('max_event_time', 'current_timestamp') }} as bigint
    )                 as lag_seconds,

    -- Against the committed seeds this is always breached because the seeds are
    -- a fixed historical window and lag therefore grows forever that is correct
    -- on static data and it is why the dbt tests assert the shape of this table
    -- (coverage, non-null, positive counts) rather than that lag is within SLA
    -- A test that can only pass on live data fails CI every day and gets
    -- deleted
    case
        when cast({{ datediff_seconds('max_event_time', 'current_timestamp') }} as bigint)
             > max_lag_seconds_sla
        then true else false
    end               as sla_breached

from observations
