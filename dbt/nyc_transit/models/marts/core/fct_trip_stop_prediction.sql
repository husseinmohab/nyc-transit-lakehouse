{{
  config(
    materialized = 'incremental',
    unique_key = 'event_id',
    incremental_strategy = 'merge' if target.type != 'duckdb' else 'delete+insert',
    partition_by = ['service_date'] if target.type != 'duckdb' else none,
    tags = ['marts', 'fact']
  )
}}

/*
  one row per trip stop and feed publication

  kept at publication grain instead of collapsing to the latest prediction per
  stop because churn is the signal
  a stop whose eta moved eight times in four minutes is a different story from
  one predicted once and hit
  fct_stop_arrival_estimate does the collapsing for whoever wants the final
  answer

  delete+insert on duckdb and merge on spark since the duckdb adapter has no
  merge strategy
  the incremental window is partition scoped so the two end up equivalent which
  is exactly why it is partition scoped and not a > on a timestamp
*/

with predictions as (

    select * from {{ ref('stg_trip_stop_predictions') }}

    {% if is_incremental() %}
      where {{ incremental_window_predicate('service_date') }}
    {% endif %}

),

enriched as (

    select
        p.event_id,
        p.service_date,
        p.feed_id,
        p.trip_id,
        p.route_id,
        p.direction_id,
        p.vehicle_id,
        p.stop_id,
        p.platform_stop_id,
        p.platform_direction,
        p.stop_sequence,
        p.feed_timestamp,
        p.feed_timestamp_ms,
        p.predicted_arrival_at,
        p.predicted_departure_at,
        p.arrival_delay_seconds,
        p.arrival_uncertainty_seconds,
        p.prediction_horizon_seconds,
        p.is_skipped,
        p.is_cancelled_trip,
        p.is_added_trip,
        p.ingest_timestamp,
        p.ingest_lag_seconds,

        -- Dimension attributes denormalised onto the fact A left join always:
        -- an inner join would drop predictions for a route the static feed has
        -- not published yet which is when you most want to see them
        r.route_short_name,
        r.division,
        s.stop_name,
        s.borough,

        -- Horizon buckets defined once every accuracy question is "how good
        -- are predictions N minutes out", and three analysts would otherwise
        -- pick three bucketings
        case
            when p.prediction_horizon_seconds is null then 'unknown'
            when p.prediction_horizon_seconds < 0 then 'past'
            when p.prediction_horizon_seconds < 120 then '0-2min'
            when p.prediction_horizon_seconds < 300 then '2-5min'
            when p.prediction_horizon_seconds < 600 then '5-10min'
            when p.prediction_horizon_seconds < 1800 then '10-30min'
            else '30min+'
        end as horizon_bucket,

        -- Three-valued A cancellation carries no stop at all so "did the stop
        -- resolve?" has no answer for it; collapsing that to false would make
        -- every cancellation look like a broken join and permanently depress
        -- the join-coverage metric
        case
            when p.stop_id is null then null
            when s.stop_id is null then false
            else true
        end as stop_resolved,

        current_timestamp as dbt_updated_at

    from predictions p
    left join {{ ref('dim_route') }} r on p.route_id = r.route_id
    left join {{ ref('dim_stop') }} s on p.stop_id = s.stop_id

)

select * from enriched
