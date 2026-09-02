{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['exchange_id', 'symbol', 'bar_start'],
        properties={
            'format': "'PARQUET'",
            'format_version': '2',
            'write_compression': "'ZSTD'",
            'partitioning': "ARRAY['day(bar_start)', 'bucket(8, symbol)']"
        }
    )
}}

/*
    Per-minute liquidity metrics from the quote stream.

    The headline field is `time_weighted_spread_bps`. A simple average of the
    spread across quote updates is not just imprecise, it is biased: quotes
    tighten in bursts of many rapid updates and widen into long quiet
    stretches, so counting each update equally over-represents exactly the
    moments when liquidity was best. Weighting by how long each quote was the
    prevailing top of book is the measure that actually answers "what spread
    would I have paid".

    Both are computed and both are kept, because the gap between them is
    itself informative -- a large divergence means the quote rate was very
    uneven within the bar.
*/

with quotes as (

    select * from {{ ref('slv_quotes') }}
    where prevailing_ms is not null
      and prevailing_ms >= 0
    {% if is_incremental() %}
      and bar_start_1m >= (
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

        count(*)                                            as quote_count,
        sum(prevailing_ms)                                  as covered_ms,

        avg(spread_bps)                                     as mean_spread_bps,
        case
            when sum(prevailing_ms) > 0
            then sum(spread_bps * prevailing_ms) / sum(prevailing_ms)
        end                                                 as time_weighted_spread_bps,

        min(spread_bps)                                     as min_spread_bps,
        max(spread_bps)                                     as max_spread_bps,
        approx_percentile(spread_bps, 0.5)                  as median_spread_bps,

        avg(bid_quantity + ask_quantity)                    as mean_touch_depth,
        avg(touch_imbalance)                                as mean_touch_imbalance,
        max_by(mid_price, event_time)                       as closing_mid_price,

        -- Updates the venue sent that we never received. The only place in the
        -- warehouse this is visible.
        sum(missed_updates_before)                          as missed_updates

    from quotes
    group by 1, 2, 3

),

final as (

    select
        *,
        -- Fraction of the minute for which we actually held a quote. Below one
        -- means a gap in coverage, which makes the time-weighted number less
        -- trustworthy and should be surfaced rather than hidden.
        cast(least(covered_ms / 60000.0, 1.0) as decimal(9, 6)) as quote_coverage_ratio,
        current_timestamp                                       as _built_at

    from aggregated

)

select * from final
