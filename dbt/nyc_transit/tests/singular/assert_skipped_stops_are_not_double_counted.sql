/*
  Consistency between the hourly rollup and the daily quality summary

  Two models count skipped stops from the same fact by different routes (one
  groups by hour the other by day) they must agree per route-day or one of
  the group-bys is wrong the classic symptom being a join that fans out and
  inflates one of them
*/

with hourly as (

    select
        service_date,
        route_id,
        sum(skipped_stop_count) as skipped_from_hourly
    from {{ ref('agg_route_service_hourly') }}
    group by service_date, route_id

),

daily as (

    select
        service_date,
        route_id,
        skipped_count as skipped_from_daily
    from {{ ref('dq_prediction_quality_daily') }}

)

select
    coalesce(h.service_date, d.service_date) as service_date,
    coalesce(h.route_id, d.route_id)         as route_id,
    h.skipped_from_hourly,
    d.skipped_from_daily
from hourly h
full outer join daily d
    on h.service_date = d.service_date
    and h.route_id = d.route_id
-- The hourly rollup drops trip-stops with a null final ETA (it groups by
-- arrival hour) so hourly <= daily is expected hourly > daily is not
where coalesce(h.skipped_from_hourly, 0) > coalesce(d.skipped_from_daily, 0)
