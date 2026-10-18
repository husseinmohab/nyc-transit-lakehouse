{{ config(materialized = 'table', tags = ['marts', 'quality']) }}

/*
  per route per day prediction quality

  quality here means stability and coverage never accuracy
  accuracy needs a ground truth arrival time and this pipeline has none
  you could derive an actual arrival off the last prediction before a STOPPED_AT
  position but that is an estimate and calling an estimate actual is how a data
  product starts lying
*/

with base as (

    select
        service_date,
        route_id,
        revision_count,
        prediction_churn_seconds,
        tracking_duration_seconds,
        final_arrival_uncertainty_seconds,
        is_skipped,
        is_cancelled_trip,
        final_predicted_arrival_at
    from {{ ref('fct_stop_arrival_estimate') }}

)

select
    service_date,
    route_id,

    count(*) as trip_stop_count,
    sum(case when final_predicted_arrival_at is null then 1 else 0 end) as missing_eta_count,
    cast(sum(case when final_predicted_arrival_at is null then 1 else 0 end) as double)
        / nullif(count(*), 0) as missing_eta_rate,

    sum(case when is_skipped then 1 else 0 end) as skipped_count,
    cast(sum(case when is_skipped then 1 else 0 end) as double)
        / nullif(count(*), 0) as skipped_rate,

    sum(case when is_cancelled_trip then 1 else 0 end) as cancelled_count,

    avg(cast(revision_count as double))                    as avg_revision_count,
    avg(abs(cast(prediction_churn_seconds as double)))     as avg_abs_churn_seconds,
    max(abs(cast(prediction_churn_seconds as double)))     as max_abs_churn_seconds,
    avg(cast(final_arrival_uncertainty_seconds as double)) as avg_uncertainty_seconds,
    avg(cast(tracking_duration_seconds as double))         as avg_tracking_seconds,

    -- A single 0-100 stability score so a dashboard has one number to trend
    -- Composite scores invite over-reading so the weights are written out and
    -- the components stay in the table beside it
    cast(
        100.0
        - least(40.0, avg(abs(cast(prediction_churn_seconds as double))) / 15.0)
        - least(30.0, avg(cast(revision_count as double)) * 2.0)
        - least(30.0, 100.0 * cast(sum(case when is_skipped then 1 else 0 end) as double)
                              / nullif(count(*), 0))
        as double
    ) as stability_score,

    current_timestamp as dbt_updated_at

from base
group by service_date, route_id
