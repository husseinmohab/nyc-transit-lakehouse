/*
  A cancelled trip carries no stop_time_updates so its normalized rows must
  have a null stop_id the normalizer emits one placeholder row per cancellation
  so the cancellation stays visible

  Conversely a row with a stop_sequence must not be marked cancelled that
  combination would mean the trip-level and stop-level schedule relationships
  were read off different entities
*/

select
    event_id,
    trip_id,
    stop_id,
    stop_sequence,
    is_cancelled_trip
from {{ ref('stg_trip_stop_predictions') }}
where is_cancelled_trip = true
  and stop_sequence is not null

union all

/*
  The complement a scheduled row must always carry a stop Asserting only one
  direction would let a bug that nulls every stop_id pass because the not_null
  test is scoped to non-cancelled rows and a row mislabelled as cancelled would
  escape both
*/
select
    event_id,
    trip_id,
    stop_id,
    stop_sequence,
    is_cancelled_trip
from {{ ref('stg_trip_stop_predictions') }}
where is_cancelled_trip = false
  and stop_id is null
