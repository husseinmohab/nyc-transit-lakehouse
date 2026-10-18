/*
  A cancelled trip carries no stop_time_updates so its normalized rows must
  have a null platform_stop_id the normalizer emits one placeholder row per
  cancellation so the cancellation stays visible

  A row naming a stop must not be marked cancelled that combination would mean
  the trip-level and stop-level schedule relationships were read off different
  entities

  Scoped on platform_stop_id and not on stop_sequence nyct leaves
  stop_sequence unset on most stop_time_updates so scoping on it would check
  about six percent of production rows
*/

select
    event_id,
    trip_id,
    platform_stop_id,
    stop_sequence,
    is_cancelled_trip
from {{ ref('stg_trip_stop_predictions') }}
where is_cancelled_trip = true
  and platform_stop_id is not null

union all

/*
  The complement a row the feed gave a stop_time_update for must still resolve
  to a parent station Checking only the first direction would let a bug in
  parent_station that nulls every stop_id pass since the not_null test on
  stop_id is scoped the same way
*/
select
    event_id,
    trip_id,
    platform_stop_id,
    stop_sequence,
    is_cancelled_trip
from {{ ref('stg_trip_stop_predictions') }}
where platform_stop_id is not null
  and stop_id is null
