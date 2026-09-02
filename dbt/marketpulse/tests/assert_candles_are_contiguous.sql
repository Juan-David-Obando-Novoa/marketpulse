/*
    Minute bars must not skip within an active trading session.

    A missing bar is invisible in every aggregate: sums and averages simply
    have one fewer term, and nothing about the result looks wrong. Only an
    explicit gap check finds it.

    Gaps are tolerated where the tape was genuinely silent, which for a major
    pair means anything under five consecutive minutes; beyond that, the more
    likely explanation is that we were not listening.
*/

{% set max_tolerable_gap_minutes = 5 %}

with ordered as (

    select
        symbol,
        bar_start,
        lead(bar_start) over (partition by symbol order by bar_start) as next_bar_start
    from {{ ref('fct_market_1m') }}
    where bar_start >= current_timestamp - interval '7' day

)

select
    symbol,
    bar_start                                       as gap_after,
    next_bar_start,
    date_diff('minute', bar_start, next_bar_start)  as gap_minutes
from ordered
where next_bar_start is not null
  and date_diff('minute', bar_start, next_bar_start) > {{ max_tolerable_gap_minutes }}
