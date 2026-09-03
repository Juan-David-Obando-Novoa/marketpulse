{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['symbol', 'bar_start'],
        properties={'format': "'PARQUET'", 'format_version': '2'}
    )
}}

/*
    The independent check.

    Every other quality test in this project compares our data to our own
    expectations. This one compares two independently produced answers to the
    same question: the candles we aggregated from the trade tape, and the
    candles the venue published for the same window.

    That is the only test here capable of catching a whole class of bug -- a
    dropped trade, a double-counted one, a trade assigned to the wrong bar --
    because all of those are perfectly self-consistent within our own data. A
    volume difference above a fraction of a percent means we are missing trades
    the venue printed.

    Tolerances are relative, not absolute: an absolute threshold is meaningless
    across instruments whose notional differs by four orders of magnitude.
*/

with ours as (

    select
        symbol,
        bar_start,
        close_price,
        base_volume,
        trade_count,
        taker_buy_base_volume
    from {{ ref('slv_ohlcv_1m') }}
    {% if is_incremental() %}
    where bar_start >= (
        select coalesce(max(bar_start), timestamp '1970-01-01 00:00:00') - interval '48' hour
        from {{ this }}
    )
    {% endif %}

),

theirs as (

    select
        symbol,
        bar_start,
        close_price,
        base_volume,
        trade_count,
        taker_buy_base_volume
    from {{ ref('stg_binance__klines') }}
    where bar_interval = '1m'

),

compared as (

    select
        coalesce(o.symbol, t.symbol)                as symbol,
        coalesce(o.bar_start, t.bar_start)          as bar_start,

        o.close_price                               as our_close_price,
        t.close_price                               as venue_close_price,
        o.base_volume                               as our_base_volume,
        t.base_volume                               as venue_base_volume,
        o.trade_count                               as our_trade_count,
        t.trade_count                               as venue_trade_count,

        -- Relative differences. abs() because direction is not the question --
        -- being 2% under and 2% over are the same size of problem.
        abs(o.close_price - t.close_price) / nullif(t.close_price, 0)   as close_price_rel_diff,
        abs(o.base_volume - t.base_volume) / nullif(t.base_volume, 0)   as volume_rel_diff,
        o.trade_count - t.trade_count                                   as trade_count_diff,

        case
            when o.symbol is null then 'missing_from_ours'
            when t.symbol is null then 'missing_from_venue'
            else 'matched'
        end                                          as match_status

    from ours as o
    full outer join theirs as t
        on o.symbol = t.symbol and o.bar_start = t.bar_start

)

select
    *,
    case
        when match_status <> 'matched'                then 'unmatched'
        -- One tick of rounding on the close is expected: we take the last
        -- trade in the bar, the venue takes the last trade it saw.
        when coalesce(close_price_rel_diff, 0) > 0.0001 then 'price_divergence'
        -- 0.5% of volume is roughly one missed trade in a quiet minute and
        -- clearly systematic in a busy one.
        when coalesce(volume_rel_diff, 0) > 0.005       then 'volume_divergence'
        when abs(coalesce(trade_count_diff, 0)) > 2     then 'count_divergence'
        else 'ok'
    end                as reconciliation_status,
    current_timestamp  as _built_at
from compared
