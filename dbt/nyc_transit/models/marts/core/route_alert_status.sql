{{ config(materialized = 'table', tags = ['marts', 'snapshot_source']) }}

/*
  current alert status per route and the source the snapshot watches

  the mta edits alerts in place so the header changes and the old text is gone
  bronze.alerts keeps every version since the payload fingerprint is part of
  event_id but what was this routes status on tuesday needs a dimension

  this is where a dbt snapshot beats a hand rolled scd2
  current state grain an overwriting source and the only transition time
  available is when we saw it
*/

with latest_alerts as (

    select
        route_id,
        alert_entity_id,
        alert_cause,
        alert_effect,
        header_text,
        feed_timestamp,
        active_period_start,
        active_period_end,
        row_number() over (
            partition by route_id
            order by feed_timestamp desc, alert_entity_id
        ) as recency_rank
    from {{ ref('stg_alert_informed_entities') }}
    where route_id is not null

),

current_alert as (

    select
        route_id,
        alert_entity_id,
        alert_cause,
        alert_effect,
        header_text,
        feed_timestamp as alert_published_at,
        active_period_start,
        active_period_end
    from latest_alerts
    where recency_rank = 1

),

alert_counts as (

    select
        route_id,
        count(distinct alert_entity_id) as open_alert_count
    from {{ ref('stg_alert_informed_entities') }}
    where route_id is not null
    group by route_id

)

select
    r.route_id,
    r.route_short_name,
    coalesce(c.alert_effect, 'NO_ALERT') as alert_effect,
    coalesce(c.alert_cause, 'NO_CAUSE')  as alert_cause,
    c.alert_entity_id,
    c.header_text,
    c.alert_published_at,
    c.active_period_start,
    c.active_period_end,
    coalesce(a.open_alert_count, 0)      as open_alert_count,

    -- The attribute the snapshot keys its change detection on
    -- the effect enum is the signal when the feed sets it but the live subway
    -- alerts feed leaves effect and cause unset on every alert and puts the
    -- category in the header instead so the text is the fallback
    -- keying the snapshot on the header itself would version it every time
    -- someone fixed a typo, so this collapses to four coarse values
    -- the phrases come from the wording the mta actually publishes
    -- 151 distinct live headers sorted 31 disrupted 84 degraded 36 notice
    -- where notice is almost all boarding and entrance information
    case
        when c.alert_entity_id is null then 'normal'
        when c.alert_effect in ('NO_SERVICE', 'REDUCED_SERVICE') then 'disrupted'
        when c.alert_effect in ('SIGNIFICANT_DELAYS', 'DETOUR', 'MODIFIED_SERVICE') then 'degraded'
        when c.alert_effect is not null then 'notice'
        when {{ header_matches('c.header_text',
                   phrases=[', no ', 'no service', 'suspend', 'bus replace',
                            'shuttle bus'],
                   prefixes=['no ']) }} then 'disrupted'
        when {{ header_matches('c.header_text', phrases=[
                   'skip', 'runs local', 'run local', 'runs express', 'reroute',
                   'runs via', 'delay', 'runs every', 'minutes between',
                   'may slow', 'replaces']) }} then 'degraded'
        else 'notice'
    end                                  as service_status

from {{ ref('dim_route') }} r
left join current_alert c on r.route_id = c.route_id
left join alert_counts a on r.route_id = a.route_id
