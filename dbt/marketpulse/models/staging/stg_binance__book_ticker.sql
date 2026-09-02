{{ config(materialized='ephemeral') }}

/*
    Staging for the top-of-book change stream.

    The spread arithmetic lives in a macro rather than inline because spread in
    basis points is written four incompatible ways in a typical codebase and
    the results disagree by enough to matter and not enough to notice.
*/

with source as (

    select * from {{ source('bronze', 'book_ticker') }}

),

renamed as (

    select
        exchange                                       as exchange_id,
        symbol,
        update_id,

        cast(bid_price as decimal(38, 18))             as bid_price,
        cast(bid_quantity as decimal(38, 18))          as bid_quantity,
        cast(ask_price as decimal(38, 18))             as ask_price,
        cast(ask_quantity as decimal(38, 18))          as ask_quantity,

        cast({{ marketpulse.mid_price('bid_price', 'ask_price') }} as decimal(38, 18)) as mid_price,
        cast({{ marketpulse.spread_bps('bid_price', 'ask_price') }} as decimal(18, 6)) as spread_bps,
        cast(ask_price - bid_price as decimal(38, 18)) as spread_absolute,

        -- Depth imbalance at the touch: >0 means more size resting on the bid.
        case
            when (bid_quantity + ask_quantity) > 0
            then cast(
                (bid_quantity - ask_quantity) / (bid_quantity + ask_quantity)
                as decimal(18, 6)
            )
        end                                            as touch_imbalance,

        event_time,
        ingested_at                                    as producer_ingested_at,
        _ingested_at                                   as lake_ingested_at,
        _kafka_partition                               as kafka_partition,
        _kafka_offset                                  as kafka_offset,
        producer_id,
        schema_version

    from source
    where {{ marketpulse.incremental_window('event_time') }}

)

select * from renamed
