{{ config(materialized = 'view', tags = ['staging', 'static']) }}

select
    stop_id,
    stop_name,
    cast(stop_lat as double) as stop_lat,
    cast(stop_lon as double) as stop_lon,
    borough,
    served_by_routes,
    -- same bounding box as NYC_BBOX in streaming/transforms.py
    -- applied to the static stops too since a station outside it means the
    -- reference data is wrong which is a different bug from a bad vehicle fix
    case
        when cast(stop_lat as double) between 40.4 and 41.1
         and cast(stop_lon as double) between -74.3 and -73.6
        then true else false
    end as coordinates_are_plausible
from {{ source('gtfs_static', 'gtfs_stops') }}
