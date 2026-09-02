"""The Kafka publishing boundary.

Wraps ``confluent_kafka.Producer`` with the three things a raw producer does
not give you and every production pipeline eventually grows:

* **Schema binding at start-up.** Schemas are registered (or looked up) once
  when the process starts, so a contract violation is a start-up failure with
  a clear message rather than a serialisation exception at 3am.
* **A dead-letter path.** A payload that fails validation is published to the
  dead-letter topic with the exception attached. It is never dropped, and it
  never takes the process down (ADR-0006).
* **Delivery accounting.** librdkafka's ``produce`` is asynchronous and
  fire-and-forget by default; the delivery callback here is what turns a
  silent broker rejection into a counter and a log line.

The underlying client is injected through :class:`ProducerLike` so the whole
class is testable without a broker.
"""

from __future__ import annotations

import time
from types import TracebackType
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from marketpulse.contracts.models import DeadLetter, MarketDataRecord
from marketpulse.contracts.registry import (
    AvroCodec,
    SchemaRegistryClient,
    SchemaRegistryError,
    subject_for,
)
from marketpulse.logging import get_logger

if TYPE_CHECKING:
    from marketpulse.config import KafkaSettings
    from marketpulse.observability import IngestionMetrics

__all__ = ["MarketDataPublisher", "ProducerLike", "PublishResult"]

log = get_logger(__name__)


@runtime_checkable
class ProducerLike(Protocol):
    """The slice of ``confluent_kafka.Producer`` this module actually uses.

    Declaring the seam explicitly is what lets the unit tests run without a
    broker, and documents exactly how much of librdkafka we depend on.
    """

    def produce(self, topic: str, **kwargs: Any) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...

    def __len__(self) -> int: ...


class PublishResult:
    """Outcome of a publish attempt, for callers that want to branch on it."""

    __slots__ = ("error", "topic")

    def __init__(self, topic: str, error: Exception | None = None) -> None:
        self.topic = topic
        self.error = error

    @property
    def ok(self) -> bool:
        return self.error is None

    def __bool__(self) -> bool:
        return self.ok


class MarketDataPublisher:
    """Publish contract records to Kafka with schema binding and a DLQ."""

    def __init__(
        self,
        producer: ProducerLike,
        settings: KafkaSettings,
        metrics: IngestionMetrics,
        *,
        registry: SchemaRegistryClient | None = None,
        producer_id: str = "unknown",
    ) -> None:
        self._producer = producer
        self._settings = settings
        self._metrics = metrics
        self._registry = registry
        self._producer_id = producer_id
        self._codecs: dict[str, AvroCodec] = {}
        self._dlq_codec: AvroCodec | None = None

    # ------------------------------------------------------------------
    # Start-up
    # ------------------------------------------------------------------
    def bind_schemas(self, records: dict[str, type[MarketDataRecord]]) -> None:
        """Bind a codec per logical stream, registering schemas if a registry is set.

        ``records`` maps the logical stream name (``"trades"``) to the model
        class whose contract governs it. Without a registry the codecs fall
        back to bare Avro, which is the mode the unit tests and a
        registry-less local run use.
        """
        for stream, model in records.items():
            topic = self._settings.topics[stream]
            codec = AvroCodec.from_file(model.avro_schema_file)
            if self._registry is not None:
                codec = codec.with_schema_id(self._register(topic, codec.schema))
            self._codecs[stream] = codec

        dlq_topic = self._settings.topic_dlq
        dlq_codec = AvroCodec.from_file(DeadLetter.avro_schema_file)
        if self._registry is not None:
            dlq_codec = dlq_codec.with_schema_id(self._register(dlq_topic, dlq_codec.schema))
        self._dlq_codec = dlq_codec

        log.info("schemas.bound", streams=sorted(self._codecs), dead_letter_topic=dlq_topic)

    def _register(self, topic: str, schema: dict[str, Any]) -> int:
        """Register a schema, failing loudly on an incompatible change.

        Deliberately fatal. A producer that starts up with an incompatible
        contract will corrupt a downstream reader quietly for hours; refusing
        to start is the cheaper failure.
        """
        if self._registry is None:  # pragma: no cover - guarded by the caller
            raise RuntimeError("no schema registry configured")
        subject = subject_for(topic)
        if not self._registry.check_compatibility(subject, schema):
            raise SchemaRegistryError(
                f"schema for {subject} is not BACKWARD compatible with the registered "
                f"version; see ADR-0006 for the evolution policy"
            )
        schema_id = self._registry.register(subject, schema)
        log.info("schema.registered", subject=subject, schema_id=schema_id)
        return schema_id

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------
    def publish(self, stream: str, record: MarketDataRecord) -> PublishResult:
        """Serialise and publish one record. Never raises on a broker error.

        The partition key is the record's ``partition_key`` (the symbol), which
        keeps every event for an instrument on one partition and therefore in
        venue order -- the property the silver dedup and the OHLCV models both
        assume.
        """
        codec = self._codecs.get(stream)
        topic = self._settings.topics[stream]
        if codec is None:
            raise RuntimeError(f"no codec bound for stream {stream!r}; call bind_schemas() first")

        try:
            payload = codec.encode(record.to_avro_dict())
        except Exception as exc:  # noqa: BLE001 - serialisation failure must not kill the feed
            self.publish_dead_letter(
                exc,
                origin_topic=topic,
                raw_payload=record.raw_payload or repr(record),
                origin_stream=stream,
            )
            return PublishResult(topic, exc)

        enqueued_at = time.monotonic()

        def _on_delivery(err: Any, msg: Any) -> None:
            if err is not None:
                reason = getattr(err, "name", lambda: "unknown")()
                self._metrics.delivery_failures.labels(topic=topic, reason=str(reason)).inc()
                log.error("kafka.delivery_failed", topic=topic, error=str(err))
                return
            self._metrics.delivery_latency.labels(topic=topic).observe(
                time.monotonic() - enqueued_at
            )
            self._metrics.messages_published.labels(topic=topic, symbol=record.partition_key).inc()

        try:
            self._producer.produce(
                topic,
                key=record.partition_key.encode(),
                value=payload,
                on_delivery=_on_delivery,
                headers=[
                    ("schema_version", str(record.schema_version).encode()),
                    ("producer_id", self._producer_id.encode()),
                ],
            )
        except BufferError as exc:
            # librdkafka's queue is full: the broker is slower than the feed.
            # Block briefly to drain rather than dropping, then surface it.
            self._metrics.delivery_failures.labels(topic=topic, reason="local_queue_full").inc()
            log.warning("kafka.queue_full", topic=topic, queue_depth=len(self._producer))
            self._producer.poll(0.5)
            return PublishResult(topic, exc)

        self._metrics.producer_queue_depth.labels(client_id=self._settings.client_id).set(
            len(self._producer)
        )
        self._producer.poll(0)
        return PublishResult(topic)

    def publish_dead_letter(
        self,
        exc: Exception,
        *,
        origin_topic: str,
        raw_payload: str,
        origin_stream: str | None = None,
    ) -> None:
        """Route a failed message to the dead-letter topic.

        Failures here are logged and swallowed: if the DLQ itself is broken,
        taking the ingestion process down with it converts a data-quality
        incident into an availability incident.
        """
        envelope = DeadLetter.from_exception(
            exc,
            origin_topic=origin_topic,
            raw_payload=raw_payload,
            origin_stream=origin_stream,
            producer_id=self._producer_id,
        )
        self._metrics.dead_letters.labels(
            origin_topic=origin_topic, error_type=envelope.error_type
        ).inc()
        log.warning(
            "message.dead_lettered",
            origin_topic=origin_topic,
            origin_stream=origin_stream,
            error_type=envelope.error_type,
            error=envelope.error_message,
        )
        if self._dlq_codec is None:
            return
        try:
            self._producer.produce(
                self._settings.topic_dlq,
                key=origin_topic.encode(),
                value=self._dlq_codec.encode(envelope.to_avro_dict()),
            )
        except Exception as dlq_exc:  # noqa: BLE001 - a broken DLQ must not stop ingestion
            log.error("dead_letter.publish_failed", error=str(dlq_exc))

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def poll(self, timeout: float = 0.0) -> int:
        return self._producer.poll(timeout)

    def flush(self, timeout: float = 30.0) -> int:
        """Block until the queue drains. Returns messages still undelivered.

        A non-zero return on shutdown means data loss, so callers log it rather
        than treating flush as fire-and-forget.
        """
        remaining = self._producer.flush(timeout)
        if remaining:
            log.error("producer.flush_incomplete", undelivered=remaining)
        return remaining

    def __enter__(self) -> MarketDataPublisher:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.flush()
