"""Shared plumbing for the Kafka-to-Iceberg streaming jobs.

Three problems are solved here once instead of twice per job.

**Confluent framing.** ``from_avro`` knows nothing about the Schema Registry, so
the five-byte header (magic byte plus schema id) has to come off before the
Avro body is readable. That is the mirror image of what
``contracts/registry.py`` does on the producer side; the framing is defined in
one place and stripped in one place.

**Reader schema.** The job reads with the schema shipped in this package rather
than one fetched per-message from the registry. That is safe precisely because
of ADR-0006: the subject is pinned to BACKWARD compatibility, which is the
guarantee that the current reader schema can read every writer schema that has
ever been registered. Fetching per record would buy nothing and cost a lookup.

**Bad records.** A payload that fails to decode yields nulls rather than killing
the batch, and the nulls are routed to a quarantine table. A streaming job that
dies on one bad record is a streaming job that is down until someone wakes up.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from marketpulse.contracts.registry import load_schema
from marketpulse.logging import get_logger

if TYPE_CHECKING:  # pragma: no cover - import cost only matters at runtime
    from pyspark.sql import DataFrame, SparkSession

    from marketpulse.config import AppSettings

__all__ = [
    "CONFLUENT_HEADER_BYTES",
    "build_spark_session",
    "decode_avro_value",
    "quarantine_stream",
    "read_kafka_stream",
    "write_iceberg_stream",
]

log = get_logger(__name__)

#: Magic byte (1) plus big-endian schema id (4).
CONFLUENT_HEADER_BYTES = 5


def build_spark_session(app_name: str, settings: AppSettings) -> SparkSession:
    """Create a session wired to the Iceberg REST catalog and MinIO.

    Most of this also lives in ``spark-defaults.conf``. It is repeated here so
    that a job submitted outside the image -- from a notebook, or from a test --
    behaves identically to one submitted inside it.
    """
    from pyspark.sql import SparkSession  # noqa: PLC0415

    catalog = settings.iceberg.catalog_name
    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "rest")
        .config(f"spark.sql.catalog.{catalog}.uri", settings.iceberg.rest_uri)
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog}.s3.endpoint", settings.s3.endpoint)
        .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{catalog}.warehouse", settings.s3.warehouse_uri)
        .config("spark.sql.defaultCatalog", catalog)
        .config("spark.sql.session.timeZone", "UTC")
        # Streaming micro-batches are small and uniform; adaptive re-planning
        # adds latency per batch and never changes the plan.
        .config("spark.sql.adaptive.enabled", "false")
    )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session


def read_kafka_stream(
    spark: SparkSession,
    settings: AppSettings,
    topic: str,
    *,
    starting_offsets: str = "latest",
    max_offsets_per_trigger: int | None = 500_000,
) -> DataFrame:
    """Open a Kafka source with the options that matter in production.

    ``failOnDataLoss`` is left at its default of ``true``. Setting it to false
    is the usual reflex when a job crashes after retention expiry, and it is
    the wrong one: it converts a loud "you lost data" into a silent gap. The
    correct response is a backfill from bronze, not a suppressed error.

    ``maxOffsetsPerTrigger`` bounds the first batch after a long outage. Without
    it, a job restarting against three days of retention tries to process all of
    it in one micro-batch and dies on memory, repeatedly.
    """
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("kafka.security.protocol", settings.kafka.security_protocol)
        .option("includeHeaders", "true")
    )
    if max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))
    return reader.load()


def decode_avro_value(frame: DataFrame, schema_file: str) -> DataFrame:
    """Strip the Confluent header, decode the Avro body, keep the Kafka metadata.

    The Kafka coordinates (topic, partition, offset, timestamp) are carried
    through into bronze deliberately: they are what makes a row traceable back
    to the exact message that produced it, which is the difference between
    debugging a duplicate in five minutes and in a day.
    """
    from pyspark.sql import functions as F  # noqa: PLC0415, N812
    from pyspark.sql.avro.functions import from_avro  # noqa: PLC0415

    schema_json = json.dumps(load_schema(schema_file))
    body = F.expr(
        f"substring(value, {CONFLUENT_HEADER_BYTES + 1}, length(value) - {CONFLUENT_HEADER_BYTES})"
    )

    return (
        frame.select(
            F.col("topic").alias("_kafka_topic"),
            F.col("partition").alias("_kafka_partition"),
            F.col("offset").alias("_kafka_offset"),
            F.col("timestamp").alias("_kafka_timestamp"),
            F.col("key").cast("string").alias("_kafka_key"),
            # PERMISSIVE yields nulls for an undecodable record instead of
            # failing the batch; quarantine_stream() picks those up.
            from_avro(body, schema_json, {"mode": "PERMISSIVE"}).alias("record"),
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit("binance"))
    )


def quarantine_stream(decoded: DataFrame) -> DataFrame:
    """Rows whose Avro body would not decode.

    These are rare and they matter: a non-empty quarantine table means a
    producer is writing something the registered contract does not describe.
    """
    from pyspark.sql import functions as F  # noqa: PLC0415, N812

    return decoded.filter(F.col("record").isNull()).select(
        "_kafka_topic",
        "_kafka_partition",
        "_kafka_offset",
        "_kafka_timestamp",
        "_kafka_key",
        "_ingested_at",
        F.lit("avro_decode_failed").alias("_reason"),
    )


def flatten_record(decoded: DataFrame) -> DataFrame:
    """Promote the decoded struct's fields to top-level columns, keeping metadata."""
    from pyspark.sql import functions as F  # noqa: PLC0415, N812

    metadata = [c for c in decoded.columns if c.startswith("_")]
    return decoded.filter(F.col("record").isNotNull()).select("record.*", *metadata)


def write_iceberg_stream(
    frame: DataFrame,
    *,
    table: str,
    checkpoint_location: str,
    trigger_seconds: int = 60,
    query_name: str | None = None,
    output_mode: str = "append",
) -> Any:
    """Start an append-only Iceberg stream write.

    The trigger interval is the small-file lever. At a per-record or
    per-second trigger this writes kilobyte Parquet files and the maintenance
    job never catches up; at sixty seconds the files land in the tens of
    megabytes, which is close enough to the 128MB target that compaction is
    housekeeping rather than a rescue.

    ``fanout-enabled`` is ON, and it has to be -- but on its own it is not
    enough, which is the part that costs an afternoon.

    Iceberg drops the required write ordering only when fanout is enabled AND
    the table carries no sort order. Miss either half and it demands an
    ordering built from the partition transforms, which Spark's streaming write
    path cannot translate: the query dies during planning with "days(trade_time)
    ASC NULLS FIRST is not currently supported", before a single row moves. So
    the bronze streaming targets are declared WRITE UNORDERED in the DDL, and
    this option is the other half of that pair.

    The memory cost of fanout is bounded by the partition spec rather than by
    luck: a micro-batch covers about a minute, so it touches one day partition
    times whichever symbol buckets it contains -- a handful of open files, not
    one per distinct value.
    """
    return (
        frame.writeStream.format("iceberg")
        .outputMode(output_mode)
        .option("path", table)
        .option("checkpointLocation", checkpoint_location)
        .option("fanout-enabled", "true")
        .queryName(query_name or f"stream-to-{table}")
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )
