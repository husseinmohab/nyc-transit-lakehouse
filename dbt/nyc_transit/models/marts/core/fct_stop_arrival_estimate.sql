{{
  config(
    materialized = 'incremental',
    unique_key = 'trip_stop_sk',
    incremental_strategy = 'merge' if target.type != 'duckdb' else 'delete+insert',
    partition_by = ['service_date'] if target.type != 'duckdb' else none,
    tags = ['marts', 'fact']
  )
}}

/*
  one row per service_date trip and stop
  the mtas final answer plus how far it moved getting there

  prediction_churn_seconds and revision_count are the point here
  a stop whose eta drifted 400 seconds over 11 revisions is a different story
  from one predicted once and neither shows up in a table that keeps only the
  last value
*/

with predictions as (

    select * from {{ ref('stg_trip_stop_predictions') }}
    where trip_id is not null
      and stop_id is not null

    {% if is_incremental() %}
      and {{ incremental_window_predicate('service_date') }}
    {% endif %}

),

ranked as (

    select
        *,
        row_number() over (
            partition by service_date, trip_id, platform_stop_id
            order by feed_timestamp_ms desc
        ) as recency_rank,
        row_number() over (
            partition by service_date, trip_id, platform_stop_id
            order by feed_timestamp_ms asc
        ) as earliest_rank
    from predictions

),

latest as (

    select
        service_date,
        trip_id,
        platform_stop_id,
        stop_id,
        route_id,
        direction_id,
        platform_direction,
        vehicle_id,
        stop_sequence,
        feed_timestamp              as final_feed_timestamp,
        predicted_arrival_at        as final_predicted_arrival_at,
        predicted_departure_at      as final_predicted_departure_at,
        arrival_uncertainty_seconds as final_arrival_uncertainty_seconds,
        is_skipped,
        is_cancelled_trip,
        is_added_trip
    from ranked
    where recency_rank = 1

),

earliest as (

    select
        service_date,
        trip_id,
        platform_stop_id,
        feed_timestamp             as first_feed_timestamp,
        predicted_arrival_at       as first_predicted_arrival_at,
        prediction_horizon_seconds as first_prediction_horizon_seconds
    from ranked
    where earliest_rank = 1

),

revisions as (

    select
        service_date,
        trip_id,
        platform_stop_id,
        count(*)                             as revision_count,
        count(distinct predicted_arrival_at) as distinct_eta_count
    from predictions
    group by service_date, trip_id, platform_stop_id

)

select
    md5(
        cast(l.service_date as varchar) || '|' ||
        l.trip_id || '|' || l.platform_stop_id
    ) as trip_stop_sk,
    l.service_date,
    l.trip_id,
    l.route_id,
    l.direction_id,
    l.stop_id,
    l.platform_stop_id,
    l.platform_direction,
    l.vehicle_id,
    l.stop_sequence,

    e.first_feed_timestamp,
    e.first_predicted_arrival_at,
    e.first_prediction_horizon_seconds,
    l.final_feed_timestamp,
    l.final_predicted_arrival_at,
    l.final_predicted_departure_at,
    l.final_arrival_uncertainty_seconds,

    rv.revision_count,
    rv.distinct_eta_count,

    -- How far the estimate moved between the first and last publication
    -- Positive means the train got later
    cast(
        {{ datediff_seconds('e.first_predicted_arrival_at', 'l.final_predicted_arrival_at') }}
        as integer
    ) as prediction_churn_seconds,

    cast(
        {{ datediff_seconds('e.first_feed_timestamp', 'l.final_feed_timestamp') }} as integer
    ) as tracking_duration_seconds,

    l.is_skipped,
    l.is_cancelled_trip,
    l.is_added_trip,
    current_timestamp as dbt_updated_at

from latest l
inner join earliest e
    on l.service_date = e.service_date
    and l.trip_id = e.trip_id
    and l.platform_stop_id = e.platform_stop_id
inner join revisions rv
    on l.service_date = rv.service_date
    and l.trip_id = rv.trip_id
    and l.platform_stop_id = rv.platform_stop_id
