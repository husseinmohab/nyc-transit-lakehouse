/*
  Every (service_date, trip, platform) in the publication-grain fact must appear
  exactly once in the collapsed estimate fact and the other way round

  This catches a silent row loss between the two facts which a row-count
  comparison misses because the counts are supposed to differ
*/

with publication_grain as (

    select distinct service_date, trip_id, platform_stop_id
    from {{ ref('fct_trip_stop_prediction') }}
    where trip_id is not null
      and platform_stop_id is not null

),

estimate_grain as (

    select service_date, trip_id, platform_stop_id
    from {{ ref('fct_stop_arrival_estimate') }}

)

select
    coalesce(p.service_date, e.service_date)         as service_date,
    coalesce(p.trip_id, e.trip_id)                   as trip_id,
    coalesce(p.platform_stop_id, e.platform_stop_id) as platform_stop_id,
    case
        when p.trip_id is null then 'missing_from_publication_grain'
        else 'missing_from_estimate_grain'
    end                                              as problem
from publication_grain p
full outer join estimate_grain e
    on p.service_date = e.service_date
    and p.trip_id = e.trip_id
    and p.platform_stop_id = e.platform_stop_id
where p.trip_id is null or e.trip_id is null
