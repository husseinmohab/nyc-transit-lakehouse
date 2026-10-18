{{ config(materialized = 'table', tags = ['marts', 'dimension']) }}

/*
  route dimension with the static gtfs attributes plus observed activity

  scd type 1
  name colour and division change maybe once a year and the mta publishes no
  change date so an scd2 here would record when we noticed a rename which says
  more about this pipeline than about the subway
  the activity columns are as of last build and the yml says so
*/

with routes as (

    select * from {{ ref('stg_gtfs_routes') }}

),

observed as (

    select
        route_id,
        count(*)                                           as prediction_count,
        count(distinct trip_id)                            as distinct_trip_count,
        count(distinct stop_id)                            as distinct_stop_count,
        count(distinct service_date)                       as active_service_date_count,
        min(service_date)                                  as first_observed_service_date,
        max(service_date)                                  as last_observed_service_date,
        sum(case when is_skipped then 1 else 0 end)        as skipped_stop_count,
        sum(case when is_cancelled_trip then 1 else 0 end) as cancelled_trip_row_count
    from {{ ref('stg_trip_stop_predictions') }}
    where route_id is not null
    group by route_id

),

alerted as (

    select
        route_id,
        count(distinct alert_entity_id) as alert_count,
        max(feed_timestamp)             as last_alert_at
    from {{ ref('stg_alert_informed_entities') }}
    where route_id is not null
    group by route_id

)

select
    routes.route_id,
    routes.route_short_name,
    routes.route_long_name,
    routes.route_color_hex,
    routes.route_type,
    routes.route_mode,
    routes.division,

    coalesce(observed.prediction_count, 0)          as prediction_count,
    coalesce(observed.distinct_trip_count, 0)       as distinct_trip_count,
    coalesce(observed.distinct_stop_count, 0)       as distinct_stop_count,
    coalesce(observed.active_service_date_count, 0) as active_service_date_count,
    observed.first_observed_service_date,
    observed.last_observed_service_date,
    coalesce(observed.skipped_stop_count, 0)        as skipped_stop_count,
    coalesce(observed.cancelled_trip_row_count, 0)  as cancelled_trip_row_count,
    coalesce(alerted.alert_count, 0)                as alert_count,
    alerted.last_alert_at,

    -- A route in the static feed with zero realtime rows is normal the
    -- shuttles run limited hours and the local seeds cover three feeds out of
    -- eight so this is a flag not a filter
    case when observed.route_id is null then false else true end as has_realtime_data,

    current_timestamp                               as dbt_updated_at

from routes
left join observed on routes.route_id = observed.route_id
left join alerted on routes.route_id = alerted.route_id
