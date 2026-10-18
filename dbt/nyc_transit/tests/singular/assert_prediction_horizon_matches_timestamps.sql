/*
  prediction_horizon_seconds must equal (predicted_arrival_at - feed_timestamp)

  The column is computed in three places PySpark for streaming SQL for the
  backfill Python for the reference projection and seed generator this checks
  the value against its own definition from the timestamps beside it so a drift
  in any one of the three is caught here rather than by someone noticing that a
  dashboard looks odd

  One second of slack the three paths truncate rather than round and a float
  division can land either side of an integer boundary
*/

select
    event_id,
    feed_timestamp,
    predicted_arrival_at,
    prediction_horizon_seconds,
    cast({{ datediff_seconds('feed_timestamp', 'predicted_arrival_at') }} as integer)
        as recomputed_horizon_seconds
from {{ ref('fct_trip_stop_prediction') }}
where predicted_arrival_at is not null
  and abs(
        prediction_horizon_seconds
        - cast({{ datediff_seconds('feed_timestamp', 'predicted_arrival_at') }} as integer)
      ) > 1
