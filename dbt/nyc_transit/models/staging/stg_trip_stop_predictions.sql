{{
  config(
    materialized = 'view',
    tags = ['staging', 'trip_updates']
  )
}}

-- Staging renames casts and derives the parent-station key no filtering and
-- no aggregation a staging model that drops rows cannot be debugged against
-- its source

with source as (

    select * from {{ source('lakehouse', 'silver_trip_stop_predictions') }}

),

renamed as (

    select
        event_id,
        cast(service_date as date)                   as service_date,
        feed_id,
        trip_id,
        route_id,
        cast(direction_id as integer)                as direction_id,
        vehicle_id,

        -- GTFS-RT publishes platform-level stop ids ("127N") static stops.txt
        -- is keyed on the parent station ("127") Deriving the parent here means
        -- every downstream join uses one definition
        stop_id                                      as platform_stop_id,
        {{ parent_station('stop_id') }}              as stop_id,
        {{ platform_direction('stop_id') }}          as platform_direction,

        cast(stop_sequence as integer)               as stop_sequence,
        cast(feed_timestamp as timestamp)            as feed_timestamp,
        cast(feed_timestamp_ms as bigint)            as feed_timestamp_ms,
        cast(predicted_arrival_at as timestamp)      as predicted_arrival_at,
        cast(predicted_departure_at as timestamp)    as predicted_departure_at,
        cast(arrival_delay_seconds as integer)       as arrival_delay_seconds,
        cast(arrival_uncertainty_seconds as integer) as arrival_uncertainty_seconds,
        cast(prediction_horizon_seconds as integer)  as prediction_horizon_seconds,

        {{ to_boolean('is_skipped') }}               as is_skipped,
        {{ to_boolean('is_cancelled_trip') }}        as is_cancelled_trip,
        {{ to_boolean('is_added_trip') }}            as is_added_trip,

        cast(ingest_timestamp as timestamp)          as ingest_timestamp,

        -- The producer's own latency: poll time minus publication time. Surfaced
        -- here because it is the first thing anyone looks at when predictions
        -- look stale and it costs nothing
        cast(
            {{ datediff_seconds('feed_timestamp', 'ingest_timestamp') }} as integer
        )                                            as ingest_lag_seconds

    from source

)

select * from renamed
