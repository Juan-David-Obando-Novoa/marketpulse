"""The publisher's contract: never lose a message silently, never crash the feed."""

from __future__ import annotations

import pytest

from marketpulse.config import KafkaSettings
from marketpulse.contracts.models import BookTicker, Trade
from marketpulse.contracts.registry import AvroCodec
from marketpulse.ingestion.publisher import MarketDataPublisher, ProducerLike
from marketpulse.observability import IngestionMetrics
from tests.unit.fakes import FakeProducer

pytestmark = pytest.mark.unit


def _counter_value(metrics: IngestionMetrics, name: str, **labels: str) -> float:
    return metrics.registry.get_sample_value(name, labels) or 0.0


@pytest.fixture
def publisher_setup() -> tuple[MarketDataPublisher, FakeProducer, IngestionMetrics]:
    producer = FakeProducer()
    metrics = IngestionMetrics()
    publisher = MarketDataPublisher(producer, KafkaSettings(), metrics, producer_id="test-1")
    publisher.bind_schemas({"trades": Trade, "book_ticker": BookTicker})
    return publisher, producer, metrics


def test_fake_producer_satisfies_the_declared_seam() -> None:
    """If this fails, the Protocol and the real client have drifted."""
    assert isinstance(FakeProducer(), ProducerLike)


def test_publish_routes_to_the_configured_topic(
    publisher_setup: tuple[MarketDataPublisher, FakeProducer, IngestionMetrics], trade: Trade
) -> None:
    publisher, producer, _ = publisher_setup
    assert publisher.publish("trades", trade).ok
    assert producer.topics() == ["md.trades.v1"]


def test_partition_key_is_the_symbol(
    publisher_setup: tuple[MarketDataPublisher, FakeProducer, IngestionMetrics], trade: Trade
) -> None:
    """Ordering per instrument is a contract the silver models depend on."""
    publisher, producer, _ = publisher_setup
    publisher.publish("trades", trade)
    assert producer.messages[0]["key"] == b"BTCUSDT"


def test_payload_is_avro_and_round_trips(
    publisher_setup: tuple[MarketDataPublisher, FakeProducer, IngestionMetrics], trade: Trade
) -> None:
    publisher, producer, _ = publisher_setup
    publisher.publish("trades", trade)
    decoded = AvroCodec.from_file("trade.avsc").decode(producer.messages[0]["value"])
    assert decoded["trade_id"] == trade.trade_id
    assert decoded["price"] == trade.price


def test_headers_carry_provenance(
    publisher_setup: tuple[MarketDataPublisher, FakeProducer, IngestionMetrics], trade: Trade
) -> None:
    publisher, producer, _ = publisher_setup
    publisher.publish("trades", trade)
    headers = dict(producer.messages[0]["headers"])
    assert headers[b"producer_id" if isinstance(next(iter(headers)), bytes) else "producer_id"]


def test_publishing_to_an_unbound_stream_is_a_programming_error(
    publisher_setup: tuple[MarketDataPublisher, FakeProducer, IngestionMetrics], trade: Trade
) -> None:
    """Unlike bad data, a missing codec is our bug and must be loud."""
    publisher, _, _ = publisher_setup
    with pytest.raises(RuntimeError, match="no codec bound"):
        publisher.publish("klines", trade)


def test_delivery_success_increments_the_published_counter(
    publisher_setup: tuple[MarketDataPublisher, FakeProducer, IngestionMetrics], trade: Trade
) -> None:
    publisher, _, metrics = publisher_setup
    publisher.publish("trades", trade)
    assert (
        _counter_value(
            metrics,
            "marketpulse_messages_published_total",
            topic="md.trades.v1",
            symbol="BTCUSDT",
        )
        == 1.0
    )


def test_broker_rejection_is_counted_not_swallowed(trade: Trade) -> None:
    """produce() is fire-and-forget; without this the failure is invisible."""
    producer = FakeProducer(fail_delivery=True)
    metrics = IngestionMetrics()
    publisher = MarketDataPublisher(producer, KafkaSettings(), metrics, producer_id="test-1")
    publisher.bind_schemas({"trades": Trade})

    publisher.publish("trades", trade)

    assert (
        _counter_value(
            metrics,
            "marketpulse_delivery_failures_total",
            topic="md.trades.v1",
            reason="_MSG_TIMED_OUT",
        )
        == 1.0
    )
    assert (
        _counter_value(
            metrics, "marketpulse_messages_published_total", topic="md.trades.v1", symbol="BTCUSDT"
        )
        == 0.0
    )


def test_full_local_queue_is_backpressure_not_a_crash(trade: Trade) -> None:
    producer = FakeProducer(queue_limit=1)
    metrics = IngestionMetrics()
    publisher = MarketDataPublisher(producer, KafkaSettings(), metrics, producer_id="test-1")
    publisher.bind_schemas({"trades": Trade})

    assert publisher.publish("trades", trade).ok
    result = publisher.publish("trades", trade)

    assert not result.ok
    assert isinstance(result.error, BufferError)
    assert (
        _counter_value(
            metrics,
            "marketpulse_delivery_failures_total",
            topic="md.trades.v1",
            reason="local_queue_full",
        )
        == 1.0
    )


def test_dead_letter_carries_the_exception_and_the_payload(
    publisher_setup: tuple[MarketDataPublisher, FakeProducer, IngestionMetrics],
) -> None:
    publisher, producer, metrics = publisher_setup
    publisher.publish_dead_letter(
        ValueError("crossed book"),
        origin_topic="md.book_ticker.v1",
        raw_payload='{"b":"2","a":"1"}',
        origin_stream="btcusdt@bookTicker",
    )

    assert producer.topics() == ["md.dead_letter.v1"]
    envelope = AvroCodec.from_file("dead_letter.avsc").decode(producer.messages[0]["value"])
    assert envelope["error_type"] == "ValueError"
    assert envelope["raw_payload"] == '{"b":"2","a":"1"}'
    assert envelope["origin_stream"] == "btcusdt@bookTicker"
    assert (
        _counter_value(
            metrics,
            "marketpulse_dead_letters_total",
            origin_topic="md.book_ticker.v1",
            error_type="ValueError",
        )
        == 1.0
    )


def test_a_broken_dlq_does_not_take_down_ingestion(trade: Trade) -> None:
    """A data-quality incident must not be escalated into an availability one."""

    class ExplodingProducer(FakeProducer):
        def produce(self, topic: str, **kwargs: object) -> None:
            if topic.endswith("dead_letter.v1"):
                raise RuntimeError("broker unavailable")
            super().produce(topic, **kwargs)

    producer = ExplodingProducer()
    publisher = MarketDataPublisher(
        producer, KafkaSettings(), IngestionMetrics(), producer_id="test-1"
    )
    publisher.bind_schemas({"trades": Trade})

    publisher.publish_dead_letter(ValueError("x"), origin_topic="t", raw_payload="{}")
    assert publisher.publish("trades", trade).ok


def test_serialisation_failure_is_dead_lettered_not_raised(
    publisher_setup: tuple[MarketDataPublisher, FakeProducer, IngestionMetrics], trade: Trade
) -> None:
    publisher, producer, _ = publisher_setup
    # model_copy bypasses validation, which is exactly how a bad value reaches
    # the encoder in real life: something upstream of the contract went wrong.
    unencodable = trade.model_copy(update={"trade_id": object()})

    result = publisher.publish("trades", unencodable)

    assert not result.ok
    assert producer.topics() == ["md.dead_letter.v1"]


def test_context_manager_flushes_on_exit(
    publisher_setup: tuple[MarketDataPublisher, FakeProducer, IngestionMetrics], trade: Trade
) -> None:
    publisher, producer, _ = publisher_setup
    with publisher as bound:
        bound.publish("trades", trade)
    assert producer.flush_calls == 1
