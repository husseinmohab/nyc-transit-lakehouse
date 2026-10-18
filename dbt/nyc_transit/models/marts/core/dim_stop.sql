{{ config(materialized = 'table', tags = ['marts', 'dimension']) }}

/*
  station dimension at one row per parent station not per platform

  platform detail stays on the fact as platform_stop_id and platform_direction
  a northbound and a southbound platform are the same place so duplicating the
  station would double every count joined through here
*/

with stops as (

    select * from {{ ref('stg_gtfs_stops') }}

),

observed as (

    select
        stop_id,
        count(*)                                         as prediction_count,
        count(distinct route_id)                         as observed_route_count,
        count(distinct platform_stop_id)                 as observed_platform_count,
        min(feed_timestamp)                              as first_observed_at,
        max(feed_timestamp)                              as last_observed_at,
        avg(cast(arrival_uncertainty_seconds as double)) as avg_arrival_uncertainty_seconds
    from {{ ref('stg_trip_stop_predictions') }}
    where stop_id is not null
    group by stop_id

)

select
    stops.stop_id,
    stops.stop_name,
    stops.stop_lat,
    stops.stop_lon,
    stops.borough,
    stops.served_by_routes,
    stops.coordinates_are_plausible,

    -- Declared interchange size from the static data against what realtime
    -- shows A gap between the two usually means a route was rerouted and the
    -- static feed has not caught up
    length(stops.served_by_routes)
      - length(replace(stops.served_by_routes, '|', '')) + 1 as declared_route_count,

    coalesce(observed.prediction_count, 0)        as prediction_count,
    coalesce(observed.observed_route_count, 0)    as observed_route_count,
    coalesce(observed.observed_platform_count, 0) as observed_platform_count,
    observed.first_observed_at,
    observed.last_observed_at,
    observed.avg_arrival_uncertainty_seconds,
    case when observed.stop_id is null then false else true end as has_realtime_data,

    current_timestamp                             as dbt_updated_at

from stops
left join observed on stops.stop_id = observed.stop_id
