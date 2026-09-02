"""Batch loaders for the low-volume reference topics.

Klines and FX rates arrive in bursts of a few thousand rows a day, not
continuously. Running a Structured Streaming query against them would keep a
Spark application alive around the clock to move a rounding error's worth of
data; a triggered batch read is the right shape, and Dagster schedules it.

``availableNow`` is the key detail: it consumes everything currently in the
topic and then stops, which gives batch semantics on a streaming source while
still using the checkpoint for offset tracking. Restarting picks up exactly
where the last run finished.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from marketpulse.config import get_settings
from marketpulse.logging import configure_logging, get_logger
from marketpulse.streaming.common import (
    build_spark_session,
    decode_avro_value,
    flatten_record,
    read_kafka_stream,
)

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

log = get_logger(__name__)

APP_NAME = "marketpulse-batch-reference"


def load_topic_once(
    spark: SparkSession,
    *,
    topic: str,
    schema_file: str,
    table: str,
    checkpoint: str,
) -> int:
    """Drain ``topic`` into ``table`` and return, leaving no live query."""
    settings = get_settings()
    raw = read_kafka_stream(
        spark, settings, topic, starting_offsets="earliest", max_offsets_per_trigger=None
    )
    frame = flatten_record(decode_avro_value(raw, schema_file))

    query = (
        frame.writeStream.format("iceberg")
        .outputMode("append")
        .option("path", table)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    progress = query.lastProgress or {}
    rows = int(progress.get("numInputRows", 0))
    log.info("batch.loaded", topic=topic, table=table, rows=rows)
    return rows


def main() -> int:
    settings = get_settings()
    configure_logging(
        level=settings.observability.log_level,
        json_logs=settings.observability.json_logs,
        service_name=APP_NAME,
    )
    spark = build_spark_session(APP_NAME, settings)

    total = 0
    for topic, schema_file, table_name in (
        (settings.kafka.topic_klines, "kline.avsc", "klines"),
        (settings.kafka.topic_fx_rates, "fx_rate.avsc", "fx_rates"),
    ):
        total += load_topic_once(
            spark,
            topic=topic,
            schema_file=schema_file,
            table=settings.iceberg.table(settings.iceberg.bronze_namespace, table_name),
            checkpoint=f"{settings.s3.checkpoint_uri}/batch_{table_name}",
        )

    log.info("batch.complete", rows=total)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
