-- every route in the dimension needs an alert status row defaulting to normal
-- a route missing here vanishes from the snapshot and its history restarts
-- quietly when it comes back

select r.route_id
from {{ ref('dim_route') }} r
left join {{ ref('route_alert_status') }} s on r.route_id = s.route_id
where s.route_id is null
