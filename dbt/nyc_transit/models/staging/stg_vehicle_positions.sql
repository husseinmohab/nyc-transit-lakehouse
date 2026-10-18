{{ config(materialized = 'view', tags = ['staging', 'vehicle_positions']) }}

with source as (
    select * from {{ source('lakehouse', 'silver_vehicle_positions') }}
)

select
    event_id,
    cast(service_date as date) as service_date,
    feed_id,
    vehicle_id,
    trip_id,
    route_id,
    cast(direction_id as integer) as direction_id,
    cast(reported_at as timestamp) as reported_at,
    cast(reported_at_ms as bigint) as reported_at_ms,
    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude,
    cast(bearing as double) as bearing,
    cast(speed_mps as double) as speed_mps,
    current_status,

    stop_id as platform_stop_id,
    {{ parent_station('stop_id') }} as stop_id,

    cast(current_stop_sequence as integer) as current_stop_sequence,
    {{ to_boolean('position_is_plausible') }} as position_is_plausible,

    -- a vehicle with a trip_id is assigned
    -- an unassigned one is scheduled but not dispatched yet
    -- dim_vehicle_assignment tracks changes of this flag so it is defined once
    case when trip_id is not null and trip_id <> '' then true else false end as is_assigned

from source
