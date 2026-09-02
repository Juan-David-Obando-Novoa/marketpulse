/*
    FX validity intervals must never overlap.

    This is the highest-value test in the project relative to its size. An
    overlapping interval table silently multiplies rows in every range join
    that touches it, and the failure presents as a number being 1.3x too
    high -- plausible enough to be believed and subtle enough to take a week
    to trace back here.

    Returns the offending pairs, so a failure is diagnosable from the stored
    failures table without re-deriving anything.
*/

with intervals as (

    select
        rate_source,
        currency_pair,
        valid_from,
        valid_to,
        lead(valid_from) over (
            partition by rate_source, currency_pair order by valid_from
        ) as next_valid_from
    from {{ ref('slv_fx_rates') }}

)

select
    rate_source,
    currency_pair,
    valid_from,
    valid_to,
    next_valid_from,
    'interval extends past the start of its successor' as failure_reason
from intervals
where next_valid_from is not null
  and valid_to > next_valid_from
