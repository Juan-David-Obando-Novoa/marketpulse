{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['exchange_id', 'symbol', 'update_id'],
        properties={
            'format': "'PARQUET'",
            'format_version': '2',
            'partitioning': "ARRAY['day(event_time)', 'bucket(symbol, 16)']",
            'sorted_by': "ARRAY['symbol', 'event_time', 'update_id']"
        },
        on_schema_change='append_new_columns'
    )
}}

/*
    Silver quotes: deduplicated top-of-book, with the duration each quote was
    the prevailing one.

    That duration is the reason this model is not just a dedup. A naive average
    of `spread_bps` over a minute weights every update equally, which
    systematically understates the spread: quotes tighten in bursts of many
    rapid updates and widen into long quiet stretches, so the tight quotes are
    over-represented by count and under-represented by time. Time weighting is
    the correct measure and it needs the lead() computed here, once, while the
    rows are already ordered.
*/

with staged as (

    select * from {{ ref('stg_binance__book_ticker') }}

),

deduplicated as (

    {{ marketpulse.deduplicate(
        relation='staged',
        partition_by='exchange_id, symbol, update_id',
        order_by='lake_ingested_at asc, kafka_offset asc'
    ) }}

),

with_duration as (

    select
        *,
        lead(event_time) over (
            partition by exchange_id, symbol
            order by event_time, update_id
        ) as next_event_time,

        -- Order-book updates the venue processed between this top-of-book
        -- message and the previous one. NOT lost messages: `u` is the book's
        -- update id and the book changes at every depth, while a top-of-book
        -- message is only emitted when the touch actually moves. A liquid
        -- instrument routinely skips tens of ids between quotes.
        update_id - lag(update_id) over (
            partition by exchange_id, symbol
            order by update_id
        ) - 1 as book_updates_between

    from deduplicated

),

final as (

    select
        exchange_id,
        symbol,
        update_id,

        bid_price,
        bid_quantity,
        ask_price,
        ask_quantity,
        mid_price,
        spread_bps,
        spread_absolute,
        touch_imbalance,

        event_time,
        next_event_time,
        -- Milliseconds this quote was the prevailing top of book. Null on the
        -- final quote of a partition, which has no successor yet.
        date_diff('millisecond', event_time, next_event_time) as prevailing_ms,

        greatest(coalesce(book_updates_between, 0), 0) as book_updates_between,

        cast({{ marketpulse.floor_to_bar('event_time', 1) }} as timestamp(6)) as bar_start_1m,

        producer_ingested_at,
        lake_ingested_at,
        kafka_partition,
        kafka_offset,
        producer_id

    from with_duration

)

select * from final
