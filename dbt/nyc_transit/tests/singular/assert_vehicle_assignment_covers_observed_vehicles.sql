-- every vehicle with an observed position has to show up in the scd2 dimension
-- the dimension collapses observations into runs so a vehicle falling out
-- entirely means the window partitioning dropped it most likely on a null key

with observed as (
  select distinct vehicle_key from {{ ref('stg_vehicle_positions') }}
  where vehicle_key is not null
),
dimensioned as (
  select distinct vehicle_key from {{ ref('dim_vehicle_assignment') }}
)
select o.vehicle_key
from observed o
left join dimensioned d on o.vehicle_key = d.vehicle_key
where d.vehicle_key is null
