/*
  The SCD2 invariant no vehicle may have two versions valid at the same instant

  A self-join on strict interval overlap if this returns rows a point-in-time
  lookup can return two dimension rows for one vehicle and every fact joined
  through it double-counts
*/

select
    a.vehicle_id,
    a.vehicle_assignment_sk as sk_a,
    b.vehicle_assignment_sk as sk_b,
    a.valid_from            as valid_from_a,
    a.valid_to              as valid_to_a,
    b.valid_from            as valid_from_b,
    b.valid_to              as valid_to_b

from {{ ref('dim_vehicle_assignment') }} a
inner join {{ ref('dim_vehicle_assignment') }} b
    on a.vehicle_id = b.vehicle_id
    and a.vehicle_assignment_sk <> b.vehicle_assignment_sk
    -- Half-open [from, to) overlap a starts before b ends and b starts before
    -- a ends Equality is not an overlap because a's valid_to equals its
    -- successor's valid_from by construction.
    and a.valid_from < b.valid_to
    and b.valid_from < a.valid_to
