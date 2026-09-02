-- ---------------------------------------------------------------------------
-- Bronze layer DDL.
--
-- Tables are created explicitly rather than inferred by the first streaming
-- write. Letting a writer create the table means the partition spec, the sort
-- order and the table properties are whatever that writer happened to default
-- to -- and changing a partition spec after the fact, while Iceberg supports
-- it, means the old data keeps the old layout forever.
--
-- Run once at bootstrap:  spark-sql -f bronze.sql
-- ---------------------------------------------------------------------------

CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze;
CREATE NAMESPACE IF NOT EXISTS lakehouse.silver;
CREATE NAMESPACE IF NOT EXISTS lakehouse.gold;
CREATE NAMESPACE IF NOT EXISTS lakehouse.ops;

-- ---------------------------------------------------------------------------
-- Trades: the highest-volume table in the platform.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lakehouse.bronze.trades (
    schema_version   INT       COMMENT 'Contract version the producer wrote with',
    exchange         STRING    COMMENT 'Venue slug, lower case',
    symbol           STRING    COMMENT 'Venue-native instrument symbol',
    trade_id         BIGINT    COMMENT 'Venue trade id; unique per (exchange, symbol)',
    price            DECIMAL(38, 18),
    quantity         DECIMAL(38, 18),
    quote_quantity   DECIMAL(38, 18) COMMENT 'price * quantity, computed at the edge',
    buyer_is_maker   BOOLEAN   COMMENT 'True when the buyer rested; i.e. sell-side aggression',
    trade_time       TIMESTAMP COMMENT 'Matching-engine time. The event time for all windowing',
    event_time       TIMESTAMP COMMENT 'Venue websocket envelope time',
    ingested_at      TIMESTAMP COMMENT 'Producer accept time',
    producer_id      STRING,
    raw_payload      STRING    COMMENT 'Verbatim venue JSON (ADR-0003)',
    _kafka_topic     STRING,
    _kafka_partition INT,
    _kafka_offset    BIGINT    COMMENT 'With partition, the exact message this row came from',
    _kafka_timestamp TIMESTAMP,
    _kafka_key       STRING,
    _ingested_at     TIMESTAMP COMMENT 'Spark micro-batch time; differs from ingested_at by the Kafka hop',
    _source          STRING
)
USING iceberg
-- days() rather than hours(): at five symbols an hourly partition holds a few
-- thousand rows, and thousands of tiny partitions cost more in metadata than
-- they save in pruning. bucket(16, symbol) gives symbol-predicate pruning
-- without the unbounded partition count that partitioning BY symbol would.
PARTITIONED BY (days(trade_time), bucket(16, symbol))
TBLPROPERTIES (
    'format-version'                      = '2',
    'write.parquet.compression-codec'     = 'zstd',
    'write.target-file-size-bytes'        = '134217728',
    -- 'none', not 'hash': hash distribution makes Iceberg demand a
    -- distribution and ordering built from the partition transforms, and
    -- Spark's streaming write path cannot translate days(...) into a Catalyst
    -- expression. Batch writes can; streaming ones fail during planning.
    'write.distribution-mode'             = 'none',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'= '20',
    -- Streaming appends create a snapshot per micro-batch: 1440 a day per
    -- table. Without expiry the metadata tree outgrows the data.
    'history.expire.max-snapshot-age-ms'  = '604800000',
    'comment'                             = 'Append-only tape of executed trades. Never updated. Reprocessing source for silver.'
);

-- WRITE UNORDERED, and not by preference.
--
-- Iceberg skips the required write ordering only when fanout writers are
-- enabled AND the table is unsorted. A sorted table makes it demand an
-- ordering built from the partition transforms, and Spark's streaming write
-- path cannot translate days(...) into a Catalyst expression -- the query then
-- fails during planning with "days(trade_time) ASC NULLS FIRST is not
-- currently supported", before a single row is written.
--
-- What is actually lost is small: pruning here is carried by the partition
-- spec (day plus symbol bucket), and within-file sorting is a second-order
-- optimisation on top of it. Re-sorting belongs to compaction anyway, as a
-- Spark rewrite_data_files call with an explicit sort order -- not to a
-- 60-second micro-batch, which would pay for a shuffle every minute.
ALTER TABLE lakehouse.bronze.trades WRITE UNORDERED;

-- ---------------------------------------------------------------------------
-- Top of book.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lakehouse.bronze.book_ticker (
    schema_version   INT,
    exchange         STRING,
    symbol           STRING,
    update_id        BIGINT    COMMENT 'Monotonic per symbol; gaps are lost updates',
    bid_price        DECIMAL(38, 18),
    bid_quantity     DECIMAL(38, 18),
    ask_price        DECIMAL(38, 18),
    ask_quantity     DECIMAL(38, 18),
    event_time       TIMESTAMP,
    ingested_at      TIMESTAMP,
    producer_id      STRING,
    raw_payload      STRING,
    _kafka_topic     STRING,
    _kafka_partition INT,
    _kafka_offset    BIGINT,
    _kafka_timestamp TIMESTAMP,
    _kafka_key       STRING,
    _ingested_at     TIMESTAMP,
    _source          STRING
)
USING iceberg
PARTITIONED BY (days(event_time), bucket(16, symbol))
TBLPROPERTIES (
    'format-version'                      = '2',
    'write.parquet.compression-codec'     = 'zstd',
    'write.target-file-size-bytes'        = '134217728',
    -- 'none', not 'hash': hash distribution makes Iceberg demand a
    -- distribution and ordering built from the partition transforms, and
    -- Spark's streaming write path cannot translate days(...) into a Catalyst
    -- expression. Batch writes can; streaming ones fail during planning.
    'write.distribution-mode'             = 'none',
    'history.expire.max-snapshot-age-ms'  = '604800000',
    'comment'                             = 'Best bid and offer change stream. One row per top-of-book change.'
);

-- Unsorted for the same reason as trades above.
ALTER TABLE lakehouse.bronze.book_ticker WRITE UNORDERED;

-- ---------------------------------------------------------------------------
-- Bring already-created tables in line with the properties above.
--
-- CREATE TABLE IF NOT EXISTS does nothing to a table that already exists, so
-- a property added after the first bootstrap would never reach it -- and this
-- one is the difference between the streaming jobs running and failing during
-- planning. Re-running this file has to converge, not just not-fail.
-- ---------------------------------------------------------------------------
ALTER TABLE lakehouse.bronze.trades SET TBLPROPERTIES ('write.distribution-mode' = 'none');
ALTER TABLE lakehouse.bronze.book_ticker SET TBLPROPERTIES ('write.distribution-mode' = 'none');

-- ---------------------------------------------------------------------------
-- Venue-published candles. Low volume, and the independent reference our own
-- aggregations are reconciled against.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lakehouse.bronze.klines (
    schema_version        INT,
    exchange              STRING,
    symbol                STRING,
    interval              STRING,
    open_time             TIMESTAMP,
    close_time            TIMESTAMP,
    open                  DECIMAL(38, 18),
    high                  DECIMAL(38, 18),
    low                   DECIMAL(38, 18),
    close                 DECIMAL(38, 18),
    volume                DECIMAL(38, 18),
    quote_volume          DECIMAL(38, 18),
    trade_count           BIGINT,
    taker_buy_base_volume DECIMAL(38, 18),
    taker_buy_quote_volume DECIMAL(38, 18),
    is_closed             BOOLEAN,
    ingested_at           TIMESTAMP,
    producer_id           STRING,
    raw_payload           STRING,
    _kafka_topic          STRING,
    _kafka_partition      INT,
    _kafka_offset         BIGINT,
    _kafka_timestamp      TIMESTAMP,
    _kafka_key            STRING,
    _ingested_at          TIMESTAMP,
    _source               STRING
)
USING iceberg
-- Partitioned by month: a backfill of two years of 1m candles for five symbols
-- is roughly five million rows, which is a handful of files per month and
-- nothing per day.
PARTITIONED BY (months(open_time), interval)
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.parquet.compression-codec' = 'zstd',
    'comment'                         = 'Venue OHLCV. Backfill source and the reconciliation reference for our own candles.'
);

-- ---------------------------------------------------------------------------
-- Reference FX. Tiny, unpartitioned, read by every currency conversion.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lakehouse.bronze.fx_rates (
    schema_version  INT,
    source          STRING,
    base_currency   STRING,
    quote_currency  STRING,
    rate            DECIMAL(38, 18),
    valid_from      TIMESTAMP COMMENT 'Inclusive',
    valid_to        TIMESTAMP COMMENT 'Exclusive; intervals tile without overlap',
    ingested_at     TIMESTAMP,
    producer_id     STRING,
    raw_payload     STRING,
    _kafka_topic    STRING,
    _kafka_partition INT,
    _kafka_offset   BIGINT,
    _kafka_timestamp TIMESTAMP,
    _kafka_key      STRING,
    _ingested_at    TIMESTAMP,
    _source         STRING
)
USING iceberg
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.parquet.compression-codec' = 'zstd',
    'comment'                         = 'Official reference rates as validity intervals, not daily points.'
);

-- ---------------------------------------------------------------------------
-- Operational tables.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lakehouse.ops.decode_quarantine (
    _kafka_topic     STRING,
    _kafka_partition INT,
    _kafka_offset    BIGINT,
    _kafka_timestamp TIMESTAMP,
    _kafka_key       STRING,
    _ingested_at     TIMESTAMP,
    _reason          STRING
)
USING iceberg
PARTITIONED BY (days(_ingested_at))
TBLPROPERTIES (
    'format-version'          = '2',
    'write.distribution-mode' = 'none',
    'comment'                 = 'Messages whose Avro body would not decode. A non-empty day here means a producer is writing something the registered contract does not describe.'
);

-- ---------------------------------------------------------------------------
-- Instrument metadata as observed from the venue's exchangeInfo endpoint.
--
-- Append-only observations rather than a mutable current-state table: the
-- venue changes tick size and lot size without announcement, and a trade that
-- looks invalid against today's filters is usually valid against the ones in
-- force when it printed. The dbt snapshot over this table turns the
-- observations into a slowly-changing dimension.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lakehouse.bronze.instrument_metadata (
    symbol       STRING,
    base_asset   STRING,
    quote_asset  STRING,
    status       STRING    COMMENT 'TRADING, HALT, BREAK, ...',
    tick_size    DECIMAL(38, 18) COMMENT 'Minimum price increment in force at observed_at',
    step_size    DECIMAL(38, 18) COMMENT 'Minimum quantity increment',
    min_notional DECIMAL(38, 18),
    observed_at  TIMESTAMP COMMENT 'When we asked the venue, not when the venue changed it',
    raw_payload  STRING
)
USING iceberg
PARTITIONED BY (days(observed_at))
TBLPROPERTIES (
    'format-version' = '2',
    'comment'        = 'Observations of venue instrument filters. Source for the SCD2 snapshot.'
);

ALTER TABLE lakehouse.ops.decode_quarantine
    SET TBLPROPERTIES ('write.distribution-mode' = 'none');
