{{ config(materialized = 'view', tags = ['staging', 'alerts']) }}

with source as (
  select * from {{ source('lakehouse', 'silver_alert_informed_entities') }}
)
select
  alert_event_id,
  cast(service_date as date) as service_date,
  entity_id as alert_entity_id,
  cast(feed_timestamp as timestamp) as feed_timestamp,
  cast(feed_timestamp_ms as bigint) as feed_timestamp_ms,
  cast(informed_ordinal as integer) as informed_ordinal,
  agency_id,
  route_id,
  stop_id as platform_stop_id,
  {{ parent_station('stop_id') }} as stop_id,
  trip_id,
  cast(direction_id as integer) as direction_id,
  alert_cause,
  alert_effect,
  header_text,
  cast(active_period_start as timestamp) as active_period_start,
  cast(active_period_end as timestamp) as active_period_end,

  -- an empty informed_entity array means the alert applies system wide
  -- posexplode_outer keeps it as a row of nulls so name the condition here
  -- instead of re deriving it downstream
  case
      when route_id is null and stop_id is null and trip_id is null then true
      else false
  end as is_systemwide

from source
