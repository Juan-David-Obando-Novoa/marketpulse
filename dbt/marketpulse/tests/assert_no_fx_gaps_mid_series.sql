/*
    The FX interval series must be gapless once it has started.

    A gap means a minute in fct_market_1m resolves to a null rate and its
    COP-denominated columns silently disappear from any sum. Leading nulls
    (before the first published rate) are legitimate and excluded; a hole in
    the middle is not.
*/

with intervals as (

    select
        currency_pair,
        valid_to,
        lead(valid_from) over (partition by currency_pair order by valid_from) as next_valid_from
    from {{ ref('slv_fx_rates') }}

)

select
    currency_pair,
    valid_to     as gap_starts_at,
    next_valid_from as gap_ends_at,
    date_diff('day', valid_to, next_valid_from) as gap_days
from intervals
where next_valid_from is not null
  and next_valid_from > valid_to
