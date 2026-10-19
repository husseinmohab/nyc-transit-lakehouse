/*
  Look-ahead guard

  A predicted arrival long before the feed timestamp that carried it cannot be
  a prediction it would mean the lake holds a value that could not have been
  known when it was published

  the tolerance is not zero because the mta leaves stale trips in the feed and
  publishes stops whose eta has long passed
  measured across two live polls the furthest back was 4.4 hours and then 6.45
  hours, so the bound is 12 hours, roughly double the worst seen

  the point of this test is catching a seconds against milliseconds mix up,
  which is off by a factor of 1000 and would show up as days or years
  a 12 hour bound still catches that immediately
  how stale the feed runs is a data quality question and lives in
  dq_prediction_quality_daily, not here

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
  and prediction_horizon_seconds < -43200
