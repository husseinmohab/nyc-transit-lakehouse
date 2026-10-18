/*
  Join coverage asserted as a threshold rather than as 100%

  The committed stop seed is a curated subset of stops.txt (see
  docs/limitations.md) and production loads the full file so demanding 100%
  would be a test that only passes locally 99% is high enough that a broken
  parent_station macro which would drop coverage to near zero fails loudly,
  while a legitimately missing station does not

  Returning a row means coverage fell below the threshold
*/

with coverage as (

    select
        count(*)                                       as total_rows,
        sum(case when stop_resolved then 1 else 0 end) as resolved_rows
    from {{ ref('fct_trip_stop_prediction') }}
    -- Only rows that have a stop to resolve Including cancellations which
    -- carry no stop would measure the cancellation rate instead
    where stop_id is not null

)

select
    total_rows,
    resolved_rows,
    cast(resolved_rows as double) / nullif(total_rows, 0) as coverage_rate
from coverage
where cast(resolved_rows as double) / nullif(total_rows, 0) < 0.99
