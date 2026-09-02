{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['symbol', 'bar_start'],
        properties={
            'format': "'PARQUET'",
            'format_version': '2',
            'write_compression': "'ZSTD'",
            'partitioning': "ARRAY['month(bar_start)', 'bucket(8, symbol)']",
            'sorted_by': "ARRAY['symbol', 'bar_start']"
        }
    )
}}

/*
    The platform's central fact: one row per instrument per minute, joining
    what traded with what was quoted.

    Trades and quotes are two different arrival processes, so the join is a
    LEFT join from the trade side and never an inner one. A minute with trades
    but no quote update is real -- it means the book did not move -- and
    dropping it would silently delete the calmest minutes from every analysis
    that touches this table, which is exactly the kind of bias nobody notices.

    Partitioned by month rather than day: at one row per symbol-minute this is
    roughly 43,000 rows per symbol per month, which is one healthy file.
*/

with ohlcv as (

    select * from {{ ref('slv_ohlcv_1m') }}
    {% if is_incremental() %}
    where bar_start >= (
        select coalesce(max(bar_start), timestamp '1970-01-01 00:00:00')
               - interval '{{ var('incremental_lookback_hours') }}' hour
        from {{ this }}
    )
    {% endif %}

),

quotes as (

    select * from {{ ref('slv_quote_metrics_1m') }}

),

fx as (

    select * from {{ ref('slv_fx_rates') }}
    where currency_pair = 'USDCOP'

),

joined as (

    select
        o.exchange_id,
        o.symbol,
        o.bar_start,
        o.bar_end,

        -- Price and volume
        o.open_price,
        o.high_price,
        o.low_price,
        o.close_price,
        o.vwap,
        o.base_volume,
        o.quote_volume,
        o.trade_count,
        o.log_return,
        o.range_bps,

        -- Flow
        o.order_flow_imbalance,
        o.taker_buy_ratio,
        o.taker_buy_base_volume,
        o.largest_trade_size,
        o.mean_trade_size,

        -- Liquidity, from the quote side. Null where the book did not move.
        q.time_weighted_spread_bps,
        q.mean_spread_bps,
        q.median_spread_bps,
        q.mean_touch_depth,
        q.mean_touch_imbalance,
        q.quote_count,
        q.quote_coverage_ratio,
        q.book_updates_skipped,

        -- Local-currency conversion via a range join on the validity interval.
        -- This is why fx rates are stored as intervals rather than daily
        -- points: no gap filling, no boundary ambiguity, no correlated
        -- subquery per row.
        fx.rate                                                     as usdcop_rate,
        cast(o.close_price * fx.rate as decimal(38, 8))             as close_price_cop,
        cast(o.quote_volume * fx.rate as decimal(38, 8))            as quote_volume_cop,

        -- Quality flags carried on the fact, so a consumer can filter without
        -- joining anything.
        o.max_ingestion_lag_ms,
        o.duplicated_trade_count,
        q.quote_count is null                                       as is_quote_gap,
        coalesce(q.quote_coverage_ratio, 0) < 0.95                  as is_low_quote_coverage,

        current_timestamp                                           as _built_at

    from ohlcv as o
    -- LEFT, always. A quiet minute is data, not an absence of data.
    left join quotes as q
        on  o.exchange_id = q.exchange_id
        and o.symbol      = q.symbol
        and o.bar_start   = q.bar_start
    left join fx
        on  o.bar_start >= fx.valid_from
        and o.bar_start <  fx.valid_to

)

select * from joined
