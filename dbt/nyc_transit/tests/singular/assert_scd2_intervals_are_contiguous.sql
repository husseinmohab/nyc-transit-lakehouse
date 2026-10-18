/*
  No gaps in the SCD2 history each version's valid_to must equal the next
  version's valid_from exactly.

  A gap means there is an instant at which the dimension claims to know nothing
  about a vehicle that was in fact being observed Overlaps are caught by
  assert_scd2_no_overlapping_intervals both are needed because a history can
  be gap-free and overlapping or disjoint and gapped
*/

with ordered as (

    select
        vehicle_id,
        vehicle_assignment_sk,
        valid_from,
        valid_to,
        lead(valid_from) over (partition by vehicle_id order by valid_from) as next_valid_from
    from {{ ref('dim_vehicle_assignment') }}

)

select *
from ordered
where next_valid_from is not null
  and valid_to <> next_valid_from
