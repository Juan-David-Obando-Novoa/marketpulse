{{ config(materialized='ephemeral') }}

/*
    Staging for venue-published candles.

    Only closed candles are promoted. The venue emits the in-flight candle on
    the websocket with is_closed = false, and letting one past this filter means
    a partial bar competing with the completed one for the same natural key --
    which the dedup would resolve by ingestion order, i.e. arbitrarily.
*/

with source as (

    select * from {{ source('bronze', 'klines') }}

),

renamed as (

    select
        exchange                                  as exchange_id,
        symbol,
        interval                                  as bar_interval,

        open_time                                 as bar_start,
        -- The venue's close_time is the last millisecond of the window. Adding
        -- one gives the exclusive bound, so bars tile without overlap.
        close_time + interval '1' millisecond     as bar_end,

        cast(open as decimal(38, 18))             as open_price,
        cast(high as decimal(38, 18))             as high_price,
        cast(low as decimal(38, 18))              as low_price,
        cast(close as decimal(38, 18))            as close_price,
        cast(volume as decimal(38, 18))           as base_volume,
        cast(quote_volume as decimal(38, 18))     as quote_volume,
        trade_count,
        cast(taker_buy_base_volume as decimal(38, 18))  as taker_buy_base_volume,
        cast(taker_buy_quote_volume as decimal(38, 18)) as taker_buy_quote_volume,

        ingested_at                               as producer_ingested_at,
        _ingested_at                              as lake_ingested_at,
        producer_id

    from source
    where is_closed
      and {{ marketpulse.incremental_window('open_time') }}

)

select * from renamed
