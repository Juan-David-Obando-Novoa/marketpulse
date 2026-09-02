"""Stream ``md.trades.v1`` into ``lakehouse.bronze.trades``.

Submitted as its own Spark application rather than sharing a session with the
book-ticker job. One query per application costs a little more memory and buys
independent failure: a decode problem on the quote stream must not stop trades
from landing, and restarting one must not reset the other's offsets.

    spark-submit src/marketpulse/streaming/bronze_trades.py
"""

from __future__ import annotations

import sys

from marketpulse.config import get_settings
from marketpulse.logging import configure_logging, get_logger
from marketpulse.streaming.common import (
    build_spark_session,
    decode_avro_value,
    flatten_record,
    quarantine_stream,
    read_kafka_stream,
    write_iceberg_stream,
)

log = get_logger(__name__)

APP_NAME = "marketpulse-bronze-trades"
SCHEMA_FILE = "trade.avsc"


def main(starting_offsets: str = "latest") -> int:
    settings = get_settings()
    configure_logging(
        level=settings.observability.log_level,
        json_logs=settings.observability.json_logs,
        service_name=APP_NAME,
    )

    spark = build_spark_session(APP_NAME, settings)
    topic = settings.kafka.topic_trades
    table = settings.iceberg.table(settings.iceberg.bronze_namespace, "trades")

    raw = read_kafka_stream(spark, settings, topic, starting_offsets=starting_offsets)
    decoded = decode_avro_value(raw, SCHEMA_FILE)

    # Two sinks off one source. Spark reads the Kafka topic once and the
    # decoded frame is reused, so quarantine costs a filter rather than a
    # second consumer.
    trades_query = write_iceberg_stream(
        flatten_record(decoded),
        table=table,
        checkpoint_location=f"{settings.s3.checkpoint_uri}/bronze_trades",
        trigger_seconds=60,
        query_name=APP_NAME,
    )
    quarantine_query = write_iceberg_stream(
        quarantine_stream(decoded),
        table=settings.iceberg.table("ops", "decode_quarantine"),
        checkpoint_location=f"{settings.s3.checkpoint_uri}/bronze_trades_quarantine",
        trigger_seconds=300,
        query_name=f"{APP_NAME}-quarantine",
    )

    log.info(
        "stream.started",
        topic=topic,
        table=table,
        starting_offsets=starting_offsets,
        trigger_seconds=60,
    )

    # awaitAnyTermination rather than awaiting the trades query alone: if the
    # quarantine sink dies, the application should exit and be restarted rather
    # than silently continuing with half its sinks.
    spark.streams.awaitAnyTermination()
    return 0 if trades_query.isActive and quarantine_query.isActive else 1


if __name__ == "__main__":  # pragma: no cover
    offsets = sys.argv[1] if len(sys.argv) > 1 else "latest"
    raise SystemExit(main(offsets))
