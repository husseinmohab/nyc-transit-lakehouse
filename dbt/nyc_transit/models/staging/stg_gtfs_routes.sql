{{ config(materialized = 'table' if target.type == 'spark' else 'view', tags = ['staging', 'static']) }}

select
    route_id,
    route_short_name,
    route_long_name,
    upper(route_color) as route_color_hex,
    cast(route_type as integer) as route_type,
    division,
    -- gtfs route_type 1 is subway and 2 is rail
    -- the staten island railway is type 2 even though it runs like a subway line
    -- so anything claiming subway has to test route_type
    case route_type
        when 1 then 'subway'
        when 2 then 'rail'
        else 'other'
    end as route_mode
from {{ source('gtfs_static', 'gtfs_routes') }}
