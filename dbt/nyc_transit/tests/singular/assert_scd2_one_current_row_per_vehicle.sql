-- exactly one open interval per vehicle
-- checked against is_current and against the sentinel because the two are
-- derived independently in the model and disagreeing is itself the bug

select vehicle_key,
       sum(case when is_current then 1 else 0 end) as current_flag_count,
       sum(case when valid_to = {{ far_future_timestamp() }} then 1 else 0 end) as open_interval_count
from {{ ref('dim_vehicle_assignment') }}
group by vehicle_key
having sum(case when is_current then 1 else 0 end) <> 1
    or sum(case when valid_to = {{ far_future_timestamp() }} then 1 else 0 end) <> 1
