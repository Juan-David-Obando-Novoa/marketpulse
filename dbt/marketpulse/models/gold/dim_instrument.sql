{{
    config(
        materialized='table',
        properties={'format': "'PARQUET'", 'format_version': '2'}
    )
}}

/*
    The instrument dimension.

    Small, fully rebuilt every run, and joined by everything. It reconciles two
    sources that are allowed to disagree: our seed (what the platform intends
    to carry) and observed activity in the lake (what actually arrived). The
    disagreement is the useful part, so it is materialised as a column rather
    than resolved silently.
*/

with reference as (

    select * from {{ ref('instrument_reference') }}

),

observed as (

    select
        symbol,
        min(event_time)      as first_trade_seen_at,
        max(event_time)      as last_trade_seen_at,
        count(*)             as lifetime_trade_count,
        sum(notional)        as lifetime_notional
    from {{ ref('slv_trades') }}
    group by 1

),

final as (

    select
        coalesce(r.symbol, o.symbol)                      as symbol,
        r.base_asset,
        r.quote_asset,
        r.instrument_class,
        r.tick_size,
        r.step_size,
        r.min_notional,
        r.listed_on,
        coalesce(r.is_tracked, false)                     as is_tracked,

        o.first_trade_seen_at,
        o.last_trade_seen_at,
        coalesce(o.lifetime_trade_count, 0)               as lifetime_trade_count,
        coalesce(o.lifetime_notional, 0)                  as lifetime_notional,

        -- The reconciliation, kept rather than resolved.
        case
            when r.symbol is null                then 'unexpected_in_lake'
            when o.symbol is null and r.is_tracked then 'tracked_but_absent'
            when o.symbol is null                then 'reference_only'
            else 'ok'
        end                                               as coverage_status,

        current_timestamp                                 as _built_at

    from reference as r
    full outer join observed as o
        on r.symbol = o.symbol

)

select * from final
