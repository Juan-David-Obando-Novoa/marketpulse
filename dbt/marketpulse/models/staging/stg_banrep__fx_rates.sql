{{ config(materialized='ephemeral') }}

/*
    Staging for the official USD/COP reference rate.

    The interval semantics are the point (see ingestion/fx_trm.py): the rate is
    valid over [valid_from, valid_to), not on a single date, because a Friday
    publication stays in force through the weekend and a holiday publication
    can span four days. Downstream conversion is therefore a range join, and
    nobody has to reimplement the gap filling.
*/

with source as (

    select * from {{ source('bronze', 'fx_rates') }}

),

renamed as (

    select
        source                                as rate_source,
        base_currency,
        quote_currency,
        base_currency || quote_currency       as currency_pair,
        cast(rate as decimal(38, 18))         as rate,

        valid_from,
        valid_to,
        date_diff('day', valid_from, valid_to) as validity_days,

        ingested_at                           as producer_ingested_at,
        _ingested_at                          as lake_ingested_at

    from source

)

select * from renamed
