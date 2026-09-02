{{
    config(
        materialized='table',
        properties={'format': "'PARQUET'", 'format_version': '2', 'write_compression': "'ZSTD'"}
    )
}}

/*
    The platform's report on itself, one row per instrument per day.

    This exists because the operational metrics in Prometheus have a fifteen-day
    retention and answer "is it broken now". This answers "was the data for the
    third of March any good", which is the question asked when someone
    questions a number, and it has to be answerable months later.

    Everything here is derived from the data itself rather than from telemetry,
    so it survives a Prometheus wipe, a producer redeploy, and a change of
    monitoring vendor.
*/

with daily as (

    select * from {{ ref('mart_symbol_daily') }}

),

reconciliation as (

    select
        symbol,
        cast(bar_start as date)                                              as trading_date,
        count(*)                                                             as reconciled_minutes,
        sum(case when reconciliation_status = 'ok' then 1 else 0 end)        as matched_minutes,
        sum(case when reconciliation_status = 'volume_divergence' then 1 else 0 end)
                                                                             as volume_divergence_minutes,
        sum(case when match_status = 'missing_from_ours' then 1 else 0 end)  as minutes_we_missed,
        max(volume_rel_diff)                                                 as worst_volume_rel_diff
    from {{ ref('fct_kline_reconciliation') }}
    group by 1, 2

),

duplicates as (

    select
        symbol,
        cast(event_time as date) as trading_date,
        count(*)                 as trades,
        sum(case when _duplicate_count > 1 then 1 else 0 end) as replayed_trades,
        avg(ingestion_lag_ms)    as mean_ingestion_lag_ms,
        approx_percentile(ingestion_lag_ms, 0.99) as p99_ingestion_lag_ms
    from {{ ref('slv_trades') }}
    group by 1, 2

),

final as (

    select
        d.symbol,
        d.trading_date,

        -- Completeness
        d.minutes_with_trades,
        d.minute_coverage_ratio,
        d.quote_gap_minutes,

        -- Correctness, measured against the venue's independent view
        r.reconciled_minutes,
        r.matched_minutes,
        r.volume_divergence_minutes,
        r.minutes_we_missed,
        r.worst_volume_rel_diff,
        cast(r.matched_minutes / nullif(r.reconciled_minutes, 0) as decimal(9, 6))
            as reconciliation_pass_ratio,

        -- Delivery semantics in practice
        dup.trades,
        dup.replayed_trades,
        cast(dup.replayed_trades / nullif(dup.trades, 0) as decimal(9, 6)) as duplicate_ratio,
        d.book_updates_skipped,

        -- Timeliness
        dup.mean_ingestion_lag_ms,
        dup.p99_ingestion_lag_ms,
        d.max_ingestion_lag_ms,

        -- One verdict per instrument-day, so a consumer can filter on a single
        -- column instead of learning six thresholds.
        case
            when d.minute_coverage_ratio < 0.90                               then 'incomplete'
            when coalesce(r.minutes_we_missed, 0) > 5                         then 'incomplete'
            when coalesce(r.matched_minutes / nullif(r.reconciled_minutes, 0), 1) < 0.98
                                                                              then 'divergent'
            when coalesce(dup.p99_ingestion_lag_ms, 0) > 30000                then 'delayed'
            when d.quote_gap_minutes > 120                                    then 'degraded_liquidity_view'
            else 'good'
        end as quality_verdict,

        current_timestamp as _built_at

    from daily as d
    left join reconciliation as r
        on d.symbol = r.symbol and d.trading_date = r.trading_date
    left join duplicates as dup
        on d.symbol = dup.symbol and d.trading_date = dup.trading_date

)

select * from final
