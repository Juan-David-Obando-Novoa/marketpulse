{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['exchange_id', 'symbol', 'trade_id'],
        partition_by=['day(event_time)', 'bucket(16, symbol)'],
        properties={
            'format': "'PARQUET'",
            'format_version': '2',
            'write_compression': "'ZSTD'",
            'partitioning': "ARRAY['day(event_time)', 'bucket(16, symbol)']",
            'sorted_by': "ARRAY['symbol', 'event_time', 'trade_id']"
        },
        on_schema_change='append_new_columns'
    )
}}

/*
    Silver trades: one row per real-world trade.

    This is where the platform's at-least-once guarantee (ADR-0007) is paid
    for. Bronze tolerates duplicates by design -- a websocket reconnect
    replays, a Spark restart reprocesses an uncommitted micro-batch, a backfill
    overlaps an existing range -- and this model is the single place they are
    collapsed.

    Everything downstream reads from here. Nothing downstream may read bronze.

    Cost note: the window function runs over the lookback window on every
    incremental run, not over the whole table, and the partition pruning from
    the incremental predicate is what keeps that bounded. On a full refresh it
    is a full sort, which is the price of a rebuild.
*/

with staged as (

    select * from {{ ref('stg_binance__trades') }}

),

deduplicated as (

    {{ marketpulse.deduplicate(
        relation='staged',
        partition_by='exchange_id, symbol, trade_id',
        order_by='lake_ingested_at asc, kafka_offset asc'
    ) }}

),

final as (

    select
        exchange_id,
        symbol,
        trade_id,

        price,
        quantity,
        notional,

        buyer_is_maker,
        is_buyer_aggressor,
        -- Signed size: the building block for order-flow imbalance downstream.
        case when is_buyer_aggressor then quantity else -quantity end as signed_quantity,

        event_time,
        venue_publish_time,
        producer_ingested_at,
        lake_ingested_at,
        ingestion_lag_ms,

        -- Bar assignment computed here rather than in each aggregate, so every
        -- downstream candle model agrees on which bar a trade belongs to.
        cast({{ marketpulse.floor_to_bar('event_time', 1) }} as timestamp(6)) as bar_start_1m,
        cast({{ marketpulse.floor_to_bar('event_time', 5) }} as timestamp(6)) as bar_start_5m,

        kafka_partition,
        kafka_offset,
        producer_id,
        schema_version,

        -- Number of bronze rows this trade appeared as. Anything above one is
        -- a replay we absorbed; aggregating it is how the duplicate rate stops
        -- being a mystery and becomes a metric.
        _duplicate_count

    from (
        select
            d.*,
            count(*) over (partition by d.exchange_id, d.symbol, d.trade_id) as _duplicate_count
        from deduplicated as d
    )

)

select * from final
