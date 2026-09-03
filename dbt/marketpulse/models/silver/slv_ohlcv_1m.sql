{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['exchange_id', 'symbol', 'bar_start'],
        properties={
            'format': "'PARQUET'",
            'format_version': '2',
            'partitioning': "ARRAY['day(bar_start)', 'bucket(symbol, 8)']",
            'sorted_by': "ARRAY['symbol', 'bar_start']"
        }
    )
}}

/*
    One-minute candles built from our own trade tape.

    Deliberately NOT taken from the venue's published klines, even though we
    ingest those. Two reasons:

    1. Building them ourselves means the same code produces candles for any
       interval and any derived measure -- VWAP, order-flow imbalance, taker
       ratio -- rather than being limited to the fields the venue chose to
       publish.
    2. It gives us two independent computations of the same quantity. The
       reconciliation model in gold compares them, and a divergence is a real
       signal: it means we dropped trades, or double-counted them, or assigned
       them to the wrong bar. Checking our data against itself would never
       reveal any of that.

    The incremental window is intentionally wider than the bar: a trade
    arriving late must rebuild its whole bar, not append to it, which is why
    the strategy is merge on the bar key rather than append.
*/

with trades as (

    select * from {{ ref('slv_trades') }}
    {% if is_incremental() %}
    where bar_start_1m >= (
        select coalesce(max(bar_start), timestamp '1970-01-01 00:00:00')
               - interval '{{ var('incremental_lookback_hours') }}' hour
        from {{ this }}
    )
    {% endif %}

),

aggregated as (

    select
        exchange_id,
        symbol,
        bar_start_1m as bar_start,
        bar_start_1m + interval '1' minute as bar_end,

        -- OHLC. first_value/last_value over an ordered window rather than
        -- min/max on time, so that two trades in the same millisecond resolve
        -- deterministically by trade_id instead of arbitrarily.
        min_by(price, (event_time, trade_id))  as open_price,
        max(price)                             as high_price,
        min(price)                             as low_price,
        max_by(price, (event_time, trade_id))  as close_price,

        sum(quantity)                          as base_volume,
        sum(notional)                          as quote_volume,
        count(*)                               as trade_count,

        {{ marketpulse.vwap('price', 'quantity') }} as vwap,

        -- Taker-side breakdown. This is what the venue's kline calls
        -- taker_buy_base_volume, and having it lets us reconcile.
        sum(case when is_buyer_aggressor then quantity else 0 end) as taker_buy_base_volume,
        sum(case when is_buyer_aggressor then notional else 0 end) as taker_buy_quote_volume,

        {{ marketpulse.order_flow_imbalance('quantity', 'buyer_is_maker') }} as order_flow_imbalance,

        max(quantity)                          as largest_trade_size,
        avg(quantity)                          as mean_trade_size,

        min(event_time)                        as first_trade_time,
        max(event_time)                        as last_trade_time,
        min(trade_id)                          as first_trade_id,
        max(trade_id)                          as last_trade_id,

        -- Data-quality carried on the fact itself, so a consumer can filter on
        -- it without joining to a separate quality table.
        avg(ingestion_lag_ms)                  as mean_ingestion_lag_ms,
        max(ingestion_lag_ms)                  as max_ingestion_lag_ms,
        sum(case when _duplicate_count > 1 then 1 else 0 end) as duplicated_trade_count,
        max(lake_ingested_at)                  as built_from_data_as_of

    from trades
    group by 1, 2, 3

),

final as (

    select
        *,
        -- Log return against the previous bar's close. Log rather than simple
        -- because returns compose additively and the volatility estimator
        -- downstream assumes it.
        ln(close_price / nullif(
            lag(close_price) over (partition by exchange_id, symbol order by bar_start), 0
        )) as log_return,

        case
            when trade_count = 0 then null
            else cast(taker_buy_base_volume / nullif(base_volume, 0) as decimal(18, 6))
        end as taker_buy_ratio,

        -- Intrabar range in basis points: a cheap volatility proxy that
        -- survives the bar where open == close.
        cast(
            (high_price - low_price) / nullif((high_price + low_price) / 2, 0) * 10000
            as decimal(18, 6)
        ) as range_bps,

        {{ marketpulse.built_at() }} as _built_at

    from aggregated

)

select * from final
