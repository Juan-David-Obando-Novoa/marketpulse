{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['rate_source', 'currency_pair', 'valid_from'],
        properties={
            'format': "'PARQUET'",
            'format_version': '2'
        }
    )
}}

/*
    Reference rates as a gapless, non-overlapping interval table.

    The source republishes rows, so this deduplicates; more importantly it
    recomputes `valid_to` from the NEXT publication rather than trusting the
    source's own upper bound. The publisher occasionally emits an interval that
    overlaps the following one after a correction, and an overlapping interval
    table silently multiplies rows in every range join that touches it -- the
    kind of bug that shows up as revenue being 1.3x too high and takes a week
    to find.

    Deriving the bound from the successor makes overlap structurally
    impossible, and the test in _silver__models.yml asserts it stays that way.
*/

with staged as (

    select * from {{ ref('stg_banrep__fx_rates') }}

),

deduplicated as (

    {{ marketpulse.deduplicate(
        relation='staged',
        partition_by='rate_source, currency_pair, valid_from',
        order_by='lake_ingested_at desc'
    ) }}

),

rebounded as (

    select
        rate_source,
        base_currency,
        quote_currency,
        currency_pair,
        rate,
        valid_from,

        -- Trust the successor, not the source's own upper bound.
        coalesce(
            lead(valid_from) over (
                partition by rate_source, currency_pair
                order by valid_from
            ),
            valid_to
        ) as valid_to,

        valid_to as source_valid_to,
        producer_ingested_at,
        lake_ingested_at

    from deduplicated

)

select
    *,
    date_diff('day', valid_from, valid_to) as validity_days,
    valid_to <> source_valid_to            as was_rebounded,
    current_timestamp                      as _built_at
from rebounded
