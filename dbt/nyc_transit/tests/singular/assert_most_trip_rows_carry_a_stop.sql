/*
  Rows with no stop are normal a cancellation has none and so does a scheduled
  trip the MTA has not started predicting yet the rockaway park shuttle sits
  that way for most of the day

  So the invariant is not that every row names a stop it is that the share
  without one stays small A normalizer that stops reading stop_time_updates or
  a rename that breaks the stop_id mapping takes this to 100% and fails here
  even though the per column not_null tests are scoped past it

  Live feeds run about 0.2% so the threshold has a lot of room

  Returning a row means too many trip rows arrived with no stop
*/

with shape as (

    select
        count(*) as total_rows,
        sum(case when platform_stop_id is null then 1 else 0 end) as rows_without_stop
    from {{ ref('stg_trip_stop_predictions') }}

)

select
    total_rows,
    rows_without_stop,
    cast(rows_without_stop as double) / nullif(total_rows, 0) as share_without_stop
from shape
where cast(rows_without_stop as double) / nullif(total_rows, 0) > 0.10
