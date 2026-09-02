"""Stream ``md.book_ticker.v1`` into ``lakehouse.bronze.book_ticker``.

Separate application from the trades job for the same reason: independent
failure and independent offsets.

    spark-submit src/marketpulse/streaming/bronze_book_ticker.py
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

APP_NAME = "marketpulse-bronze-book-ticker"
SCHEMA_FILE = "book_ticker.avsc"


def main(starting_offsets: str = "latest") -> int:
    settings = get_settings()
    configure_logging(
        level=settings.observability.log_level,
        json_logs=settings.observability.json_logs,
        service_name=APP_NAME,
    )

    spark = build_spark_session(APP_NAME, settings)
    topic = settings.kafka.topic_book_ticker
    table = settings.iceberg.table(settings.iceberg.bronze_namespace, "book_ticker")

    raw = read_kafka_stream(
        spark,
        settings,
        topic,
        starting_offsets=starting_offsets,
        # Quote updates run roughly an order of magnitude hotter than trades,
        # so the per-trigger ceiling is raised to match rather than letting the
        # job fall permanently behind after a restart.
        max_offsets_per_trigger=2_000_000,
    )
    decoded = decode_avro_value(raw, SCHEMA_FILE)

    write_iceberg_stream(
        flatten_record(decoded),
        table=table,
        checkpoint_location=f"{settings.s3.checkpoint_uri}/bronze_book_ticker",
        trigger_seconds=60,
        query_name=APP_NAME,
    )
    write_iceberg_stream(
        quarantine_stream(decoded),
        table=settings.iceberg.table("ops", "decode_quarantine"),
        checkpoint_location=f"{settings.s3.checkpoint_uri}/bronze_book_ticker_quarantine",
        trigger_seconds=300,
        query_name=f"{APP_NAME}-quarantine",
    )

    log.info("stream.started", topic=topic, table=table, starting_offsets=starting_offsets)
    spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":  # pragma: no cover
    offsets = sys.argv[1] if len(sys.argv) > 1 else "latest"
    raise SystemExit(main(offsets))
