-- the snapshots own scd2 invariant of one open row per route
-- dbt keeps dbt_valid_to itself but a bad unique_key or a concurrent snapshot
-- run can leave two open rows and nothing in dbt checks for it

select route_id, count(*) as open_row_count
from {{ ref('snap_route_alert_status') }}
where dbt_valid_to is null
group by route_id
having count(*) <> 1
