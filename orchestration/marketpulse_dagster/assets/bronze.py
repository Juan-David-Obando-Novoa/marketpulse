"""Bronze assets: the Kafka-to-Iceberg boundary.

Two different shapes live here, and conflating them is a common modelling
mistake.

The high-volume topics are consumed by **long-running streaming queries**.
Those are services, not scheduled work: they outlive any Dagster run, they have
no partitions, and "materialising" them means confirming they are alive and
recording their progress. Modelling them as a scheduled batch asset would
produce a green run every hour whether or not a single row moved.

The low-volume reference topics are genuinely batch, and are drained with
``trigger(availableNow)`` on a schedule.
"""

from __future__ import annotations

from datetime import date

from dagster import (
    AssetExecutionContext,
    MetadataValue,
    Output,
    asset,
)

from marketpulse_dagster.partitions import DAILY_PARTITIONS
from marketpulse_dagster.resources import SparkSubmitResource, TrinoResource

__all__ = ["bronze_reference_load", "bronze_streaming_queries"]

STREAMING_JOBS = {
    "bronze_trades": "/opt/marketpulse/src/marketpulse/streaming/bronze_trades.py",
    "bronze_book_ticker": "/opt/marketpulse/src/marketpulse/streaming/bronze_book_ticker.py",
}


@asset(
    group_name="bronze",
    compute_kind="spark",
    description=(
        "Ensures the Kafka-to-Iceberg streaming queries are running, and "
        "records how much they have landed. Unpartitioned because a continuous "
        "stream has no partitions -- and modelling it as an hourly batch would "
        "produce a green run whether or not a single row moved."
    ),
)
def bronze_streaming_queries(
    context: AssetExecutionContext, spark: SparkSubmitResource, trino: TrinoResource
) -> Output[None]:
    for name, script in STREAMING_JOBS.items():
        context.log.info("ensuring streaming query %s is running", name)
        spark.submit(script, detach=True)

    # Row counts are the honest health signal, not process liveness: a query
    # can be running and producing nothing, which is the failure that matters.
    trades = trino.scalar("select count(*) from lakehouse.bronze.trades", 0)
    quotes = trino.scalar("select count(*) from lakehouse.bronze.book_ticker", 0)
    latest = trino.scalar("select max(trade_time) from lakehouse.bronze.trades")

    return Output(
        None,
        metadata={
            "queries": MetadataValue.json(list(STREAMING_JOBS)),
            "bronze_trades_rows": MetadataValue.int(int(trades)),
            "bronze_book_ticker_rows": MetadataValue.int(int(quotes)),
            "latest_trade_time": MetadataValue.text(str(latest)),
        },
    )


@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="bronze",
    compute_kind="spark",
    deps=["binance_klines_backfill", "fx_reference_rates"],
    description=(
        "Drains the low-volume reference topics into Iceberg with "
        "trigger(availableNow). Batch rather than streaming because keeping a "
        "Spark application alive around the clock to move a few thousand daily "
        "rows is the wrong shape."
    ),
)
def bronze_reference_load(
    context: AssetExecutionContext, spark: SparkSubmitResource, trino: TrinoResource
) -> Output[None]:
    spark.submit("/opt/marketpulse/src/marketpulse/streaming/batch_reference.py")

    # The interpolated value is a Dagster partition key -- an ISO date the
    # framework generated from the partition definition, never a caller's
    # input. Validated here anyway, so the guarantee is local and checkable
    # rather than an assumption about the framework three files away.
    partition_date = date.fromisoformat(context.partition_key).isoformat()

    klines = trino.scalar(
        "select count(*) from lakehouse.bronze.klines "
        f"where cast(open_time as date) = date '{partition_date}'",
        0,
    )
    quarantined = trino.scalar(
        "select count(*) from lakehouse.ops.decode_quarantine "
        f"where cast(_ingested_at as date) = date '{partition_date}'",
        0,
    )

    return Output(
        None,
        metadata={
            "klines_landed": MetadataValue.int(int(klines)),
            # Non-zero means a producer is writing something the registered
            # contract does not describe. It belongs on the materialisation.
            "quarantined_messages": MetadataValue.int(int(quarantined)),
        },
    )
