{{
    config(
        materialized='ephemeral',
    )
}}

/*
    Staging: rename, cast, and nothing else.

    Bronze columns carry the venue's vocabulary and our ingestion metadata.
    This model translates both into the platform's vocabulary so that no
    downstream model ever has to know that `buyer_is_maker` reads backwards or
    that `_ingested_at` and `ingested_at` are two different clocks.

    Ephemeral on purpose (ADR-0003): if this materialised, something would
    eventually select from it instead of from silver, and it has no dedup.
*/

with source as (

    select * from {{ source('bronze', 'trades') }}

),

renamed as (

    select
        -- Natural key (ADR-0007)
        exchange                                     as exchange_id,
        symbol,
        trade_id,

        -- Measures
        cast(price as decimal(38, 18))               as price,
        cast(quantity as decimal(38, 18))            as quantity,
        cast(quote_quantity as decimal(38, 18))      as notional,

        -- The venue reports which side rested. `is_buyer_aggressor` names the
        -- direction people actually reason about, once, here.
        buyer_is_maker,
        not buyer_is_maker                           as is_buyer_aggressor,

        -- Three distinct clocks, kept distinct deliberately.
        trade_time                                   as event_time,      -- matching engine
        event_time                                   as venue_publish_time,
        ingested_at                                  as producer_ingested_at,
        _ingested_at                                 as lake_ingested_at,

        -- Provenance: enough to find the exact Kafka message behind a row.
        _kafka_topic                                 as kafka_topic,
        _kafka_partition                             as kafka_partition,
        _kafka_offset                                as kafka_offset,
        producer_id,
        schema_version,

        -- Derived lag, computed once so every consumer measures it the same way.
        date_diff('millisecond', trade_time, ingested_at) as ingestion_lag_ms

    from source
    where {{ marketpulse.incremental_window('trade_time') }}

)

select * from renamed
