{{
    config(
        materialized='table',
        properties={'format': "'PARQUET'", 'format_version': '2'}
    )
}}

/*
    Trailing-window liquidity league table.

    The analytical payoff of the whole platform: it needs the trade tape for
    turnover, the quote tape for spread, and the time-weighting from silver for
    the spread to mean anything. None of it is derivable from the venue's
    published candles alone.

    Rank is computed over the window, and instrument-days flagged as anything
    other than 'good' by mart_pipeline_health are excluded -- ranking on data
    known to be incomplete produces a confidently wrong answer, which is worse
    than a gap.
*/

{% set windows = [1, 7, 30] %}

with health as (

    select symbol, trading_date
    from {{ ref('mart_pipeline_health') }}
    where quality_verdict = 'good'

),

daily as (

    select d.*
    from {{ ref('mart_symbol_daily') }} as d
    inner join health as h
        on d.symbol = h.symbol and d.trading_date = h.trading_date
    where d.trading_date >= current_date - interval '30' day

),

windowed as (

    {% for days in windows %}
    select
        {{ days }}                                       as window_days,
        symbol,
        sum(quote_volume)                                as quote_volume,
        avg(mean_spread_bps)                             as mean_spread_bps,
        avg(mean_touch_depth)                            as mean_touch_depth,
        avg(realised_volatility_annualised)              as realised_volatility,
        sum(trade_count)                                 as trade_count,
        count(*)                                         as days_observed
    from daily
    where trading_date >= current_date - interval '{{ days }}' day
    group by 2
    {% if not loop.last %}union all{% endif %}
    {% endfor %}

),

scored as (

    select
        *,
        -- Turnover per basis point of spread: the cheapest single number that
        -- captures "deep and tight" rather than merely "busy". A venue can
        -- have enormous turnover in an instrument nobody can trade cheaply.
        quote_volume / nullif(mean_spread_bps, 0)        as liquidity_score,

        rank() over (partition by window_days order by quote_volume desc)     as turnover_rank,
        rank() over (partition by window_days order by mean_spread_bps asc)   as tightness_rank

    from windowed

)

select
    *,
    rank() over (partition by window_days order by liquidity_score desc) as liquidity_rank,
    current_timestamp                                                    as _built_at
from scored
