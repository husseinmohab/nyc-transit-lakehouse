{{ config(materialized = 'table', tags = ['marts', 'aggregate']) }}

/*
  route by service date by hour rollup and the table a dashboard reads

  built on fct_stop_arrival_estimate which is one row per trip stop rather than
  the publication grain fact
  otherwise a route whose predictions get republished more often looks busier
  counting publications instead of trip stops is the easiest way to get a
  plausible looking but wrong transit metric
*/

with estimates as (

    select
        service_date,
        route_id,
        direction_id,
        {{ dbt_date_part_hour('final_predicted_arrival_at') }} as arrival_hour,
        trip_id,
        stop_id,
        revision_count,
        prediction_churn_seconds,
        tracking_duration_seconds,
        final_arrival_uncertainty_seconds,
        is_skipped,
        is_cancelled_trip
    from {{ ref('fct_stop_arrival_estimate') }}
    where final_predicted_arrival_at is not null

)

select
    e.service_date,
    e.route_id,
    e.direction_id,
    e.arrival_hour,

    count(*)                  as trip_stop_count,
    count(distinct e.trip_id) as distinct_trip_count,
    count(distinct e.stop_id) as distinct_stop_count,

    sum(case when e.is_skipped then 1 else 0 end)        as skipped_stop_count,
    sum(case when e.is_cancelled_trip then 1 else 0 end) as cancelled_trip_stop_count,

    avg(cast(e.revision_count as double))           as avg_revision_count,
    max(e.revision_count)                           as max_revision_count,
    avg(cast(e.prediction_churn_seconds as double)) as avg_prediction_churn_seconds,

    -- Absolute churn not signed a route whose ETAs swing +120s and -120s is
    -- unstable but the signed average would report it as perfectly steady
    avg(abs(cast(e.prediction_churn_seconds as double)))    as avg_abs_prediction_churn_seconds,
    avg(cast(e.final_arrival_uncertainty_seconds as double)) as avg_final_uncertainty_seconds,
    avg(cast(e.tracking_duration_seconds as double))        as avg_tracking_duration_seconds,

    r.route_short_name,
    r.division,
    current_timestamp as dbt_updated_at

from estimates e
left join {{ ref('dim_route') }} r on e.route_id = r.route_id
group by
    e.service_date,
    e.route_id,
    e.direction_id,
    e.arrival_hour,
    r.route_short_name,
    r.division
