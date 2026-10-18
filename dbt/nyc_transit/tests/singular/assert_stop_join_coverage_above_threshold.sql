/*
  Join coverage asserted as a threshold rather than as 100%

  the stop seed holds every parent station in stops.txt so coverage is 100 on
  the committed seeds but the realtime feeds do name stop ids the static file
  has no station for at all R60 F17 R65 G25 H19 and M07 were all live and all
  absent from stops.txt
  measured 99.1% against the live feeds so the threshold sits at 98 to leave
  room while a broken parent_station macro drops coverage to near zero and
  fails loudly

  Returning a row means coverage fell below the threshold
*/

with coverage as (

    select
        count(*)                                       as total_rows,
        sum(case when stop_resolved then 1 else 0 end) as resolved_rows
    from {{ ref('fct_trip_stop_prediction') }}
    -- Only rows that have a stop to resolve A trip with no stop_time_updates
    -- has nothing to join and would measure the cancellation rate instead
    where stop_id is not null

)

select
    total_rows,
    resolved_rows,
    cast(resolved_rows as double) / nullif(total_rows, 0) as coverage_rate
from coverage
where cast(resolved_rows as double) / nullif(total_rows, 0) < 0.98
