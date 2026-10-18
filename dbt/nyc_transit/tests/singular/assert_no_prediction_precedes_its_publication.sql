/*
  Look-ahead guard

  A predicted arrival more than an hour before the feed timestamp that carried
  it cannot be a prediction it would mean the lake holds a value that could not
  have been known when it was published the MTA does legitimately publish stops
  whose ETA has just passed (a train sitting at a platform) hence the one-hour
  tolerance rather than zero

  This is the test that catches a timestamp-unit bug (seconds against
  milliseconds) which is the most common way look-ahead gets into a pipeline
  like this
*/

select
    event_id,
    feed_timestamp,
    predicted_arrival_at,
    prediction_horizon_seconds
from {{ ref('fct_trip_stop_prediction') }}
where predicted_arrival_at is not null
  and prediction_horizon_seconds < -3600
