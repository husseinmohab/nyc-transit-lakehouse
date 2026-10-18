/*
  event_id must be globally unique across record types

  make_event_id includes the record type in the hashed parts so a collision
  between a trip_update and a vehicle_position is impossible by construction
  That construction is one string concatenation away from being wrong and a
  collision would make the bronze MERGE silently drop a row
*/

with all_ids as (

    select event_id, 'trip_update' as record_type
    from {{ ref('stg_trip_stop_predictions') }}

    union all

    select event_id, 'vehicle_position' as record_type
    from {{ ref('stg_vehicle_positions') }}

    union all

    select alert_event_id as event_id, 'alert' as record_type
    from {{ ref('stg_alert_informed_entities') }}

)

select
    event_id,
    count(distinct record_type) as record_type_count
from all_ids
group by event_id
having count(distinct record_type) > 1
