{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['symbol', 'trading_date'],
        properties={'format': "'PARQUET'", 'format_version': '2', 'write_compression': "'ZSTD'"}
    )
}}

/*
    Daily summary per instrument: the table a human or a BI tool actually opens.

    Aggregated from fct_market_1m rather than from slv_trades, even though
    going back to the trades would be one fewer hop. Two reasons: the minute
    fact is where the quote-side liquidity metrics already live, and building
    daily numbers from the same minutes the intraday charts use guarantees the
    two agree. A daily figure that disagrees with the sum of its own intraday
    bars destroys trust in both.

    Realised volatility is annualised on 365 days, not 252: crypto has no
    trading calendar and applying an equity convention would understate it by
    about 20%.
*/

with minutes as (

    select * from {{ ref('fct_market_1m') }}
    {% if is_incremental() %}
    where bar_start >= (
        select coalesce(max(trading_date), date '1970-01-01') - interval '3' day
        from {{ this }}
    )
    {% endif %}

),

daily as (

    select
        symbol,
        cast(bar_start as date)                          as trading_date,

        min_by(open_price, bar_start)                    as open_price,
        max(high_price)                                  as high_price,
        min(low_price)                                   as low_price,
        max_by(close_price, bar_start)                   as close_price,

        sum(base_volume)                                 as base_volume,
        sum(quote_volume)                                as quote_volume,
        sum(trade_count)                                 as trade_count,
        {{ marketpulse.vwap('vwap', 'base_volume') }}    as daily_vwap,

        -- Liquidity. Averaged over the minutes that had a quote, which is why
        -- the coverage count is kept beside it.
        avg(time_weighted_spread_bps)                    as mean_spread_bps,
        approx_percentile(time_weighted_spread_bps, 0.5) as median_spread_bps,
        avg(mean_touch_depth)                            as mean_touch_depth,
        count(time_weighted_spread_bps)                  as minutes_with_quotes,
        count(*)                                         as minutes_with_trades,

        -- Flow
        avg(order_flow_imbalance)                        as mean_order_flow_imbalance,
        sum(taker_buy_base_volume)                       as taker_buy_base_volume,

        -- Risk
        {{ marketpulse.annualised_volatility('log_return', 1) }} as realised_volatility_annualised,
        stddev_samp(log_return)                          as minute_return_stddev,
        max(range_bps)                                   as max_minute_range_bps,

        max(usdcop_rate)                                 as usdcop_rate,

        -- Quality, carried forward so a consumer never has to go looking.
        sum(cast(is_quote_gap as integer))               as quote_gap_minutes,
        sum(book_updates_skipped)                        as book_updates_skipped,
        max(max_ingestion_lag_ms)                        as max_ingestion_lag_ms

    from minutes
    group by 1, 2

),

final as (

    select
        d.*,

        -- 1440 minutes in a day; anything less means the tape was quiet or we
        -- were not listening, and the two must be distinguishable.
        cast(d.minutes_with_trades / 1440.0 as decimal(9, 6)) as minute_coverage_ratio,

        (d.close_price - d.open_price) / nullif(d.open_price, 0) as daily_return,
        cast(d.quote_volume * d.usdcop_rate as decimal(38, 4))   as quote_volume_cop,

        -- Rank by turnover within the day. Recomputed per day rather than
        -- stored, so a new instrument does not renumber history.
        row_number() over (
            partition by d.trading_date order by d.quote_volume desc
        ) as turnover_rank,

        current_timestamp as _built_at

    from daily as d

)

select * from final
