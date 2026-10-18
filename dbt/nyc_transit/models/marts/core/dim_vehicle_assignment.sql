{{
  config(
    materialized = 'incremental',
    unique_key = 'vehicle_assignment_sk',
    incremental_strategy = 'merge' if target.type != 'duckdb' else 'delete+insert',
    partition_by = ['service_date'] if target.type != 'duckdb' else none,
    tags = ['marts', 'scd2', 'dimension']
  )
}}

/*
  scd2 of which trip and route a vehicle is working

  routes and stations change a few times a year so modelling those as scd2 is a
  textbook exercise not a real one
  vehicle assignment changes constantly and the history is useful
  which train was running the 08:14 manhattan bound L when it was delayed cannot
  be answered off a current state table

  grain is one row per vehicle per continuous run of identical tracked attributes
  tracked are trip_id route_id direction_id is_assigned

  intervals are [valid_from, valid_to) with a far future sentinel instead of NULL
  so a point in time lookup needs no coalesce and an accidental inner join cannot
  drop current rows
  a closed versions valid_to equals its successors valid_from exactly
  no gaps no overlaps and two singular tests enforce both

  not a dbt snapshot because the source already carries the event time
  a snapshot would record when dbt noticed and the history would be an artefact
  of the schedule
  snap_route_alert_status is a snapshot since the mta overwrites alert text
*/

with positions as (

    select
        vehicle_id,
        service_date,
        feed_id,
        trip_id,
        route_id,
        direction_id,
        is_assigned,
        reported_at,
        reported_at_ms
    from {{ ref('stg_vehicle_positions') }}
    where vehicle_id is not null

    {% if is_incremental() %}
      -- Partition-scoped an incremental run only reads and only replaces the
      -- service dates in its window
      and {{ incremental_window_predicate('service_date') }}
    {% endif %}

),

/*
  Collapse to one observation per (vehicle, instant) the same vehicle appears
  once per poll in every feed that carries its route and two feeds can publish
  the same position with the same timestamp without this the window functions
  below would see spurious changes caused by row order
*/
deduplicated as (

    select
        vehicle_id,
        service_date,
        trip_id,
        route_id,
        direction_id,
        is_assigned,
        reported_at,
        reported_at_ms,
        row_number() over (
            partition by vehicle_id, reported_at_ms
            order by feed_id, trip_id nulls last
        ) as observation_rank
    from positions

),

observations as (

    select
        vehicle_id,
        service_date,
        trip_id,
        route_id,
        direction_id,
        is_assigned,
        reported_at,
        reported_at_ms
    from deduplicated
    where observation_rank = 1

),

with_previous as (

    select
        vehicle_id,
        service_date,
        trip_id,
        route_id,
        direction_id,
        is_assigned,
        reported_at,
        reported_at_ms,
        lag(trip_id) over (partition by vehicle_id order by reported_at_ms)      as prev_trip_id,
        lag(route_id) over (partition by vehicle_id order by reported_at_ms)     as prev_route_id,
        lag(direction_id) over (partition by vehicle_id order by reported_at_ms) as prev_direction_id,
        lag(is_assigned) over (partition by vehicle_id order by reported_at_ms)  as prev_is_assigned
    from observations

),

/*
  Mark the first observation of each distinct run A vehicle's very first
  observation has all-NULL previous values and scd2_change_detected is
  NULL-safe so it is correctly flagged as a change rather than skipped
*/
change_flags as (

    select
        vehicle_id,
        service_date,
        trip_id,
        route_id,
        direction_id,
        is_assigned,
        reported_at,
        reported_at_ms,
        case
            when {{ scd2_change_detected(
                     ['trip_id', 'route_id', 'direction_id', 'is_assigned'],
                     previous_alias='prev'
                   ) }}
            then 1 else 0
        end as is_change
    from with_previous

),

versioned as (

    select
        vehicle_id,
        service_date,
        trip_id,
        route_id,
        direction_id,
        is_assigned,
        reported_at,
        reported_at_ms,
        sum(is_change) over (
            partition by vehicle_id
            order by reported_at_ms
            rows between unbounded preceding and current row
        ) as version_group
    from change_flags

),

/*
  One row per run with its own open and close instants valid_to comes from the
  next run's valid_from via lead(), so the intervals abut.
*/
collapsed as (

    select
        vehicle_id,
        version_group,
        min(service_date)                      as service_date,
        min(reported_at)                       as valid_from,
        min(reported_at_ms)                    as valid_from_ms,
        max(reported_at)                       as last_observed_at,
        count(*)                               as observation_count,
        max(trip_id)                           as trip_id,
        max(route_id)                          as route_id,
        max(direction_id)                      as direction_id,
        max(cast(is_assigned as integer)) = 1   as is_assigned
    from versioned
    group by vehicle_id, version_group

),

final as (

    select
        {{ scd2_surrogate_key(['vehicle_id', 'version_group'], 'valid_from') }}
                          as vehicle_assignment_sk,
        vehicle_id,
        service_date,
        trip_id,
        route_id,
        direction_id,
        is_assigned,
        valid_from,
        valid_from_ms,
        coalesce(
            lead(valid_from) over (partition by vehicle_id order by valid_from_ms),
            {{ far_future_timestamp() }}
        )                 as valid_to,
        -- is_current is derived from valid_to never stored independently two
        -- sources of truth for "current" is how SCD2 tables end up with two
        -- current rows
        case
            when lead(valid_from) over (partition by vehicle_id order by valid_from_ms) is null
            then true else false
        end               as is_current,
        last_observed_at,
        observation_count,
        cast(
            {{ datediff_seconds(
                 'valid_from',
                 'coalesce(lead(valid_from) over (partition by vehicle_id order by valid_from_ms), last_observed_at)'
               ) }} as integer
        )                 as assignment_duration_seconds,
        version_group     as scd_version,
        current_timestamp as dbt_updated_at
    from collapsed

)

select * from final
