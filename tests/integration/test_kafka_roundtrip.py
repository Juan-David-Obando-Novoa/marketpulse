"""End-to-end producer test against a real broker.

The unit suite proves the publisher behaves correctly against a fake. This
proves the fake was right about librdkafka: that idempotence and acks=all are
accepted as configured, that the Avro framing survives a real round trip, and
that the partition key actually keeps a symbol on one partition -- which the
silver dedup and the OHLCV models both assume.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from marketpulse.config import KafkaSettings
from marketpulse.contracts.models import Trade
from marketpulse.contracts.registry import AvroCodec
from marketpulse.ingestion.publisher import MarketDataPublisher
from marketpulse.observability import IngestionMetrics

pytestmark = pytest.mark.integration
UTC = timezone.utc


@pytest.fixture
def topic(kafka_bootstrap: str) -> str:
    """A throwaway topic per test run, so a rerun never reads its own history."""
    from confluent_kafka.admin import AdminClient, NewTopic  # noqa: PLC0415

    name = f"test.trades.{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    for future in admin.create_topics([NewTopic(name, num_partitions=3, replication_factor=1)]).values():
        future.result()
    yield name
    for future in admin.delete_topics([name]).values():
        future.result()


def _trade(trade_id: int, symbol: str = "BTCUSDT") -> Trade:
    now = datetime.now(tz=UTC)
    return Trade(
        symbol=symbol,
        trade_id=trade_id,
        price=Decimal("64250.10"),
        quantity=Decimal("0.0125"),
        buyer_is_maker=False,
        trade_time=now,
        event_time=now,
        producer_id="pytest",
    )


def test_records_survive_a_real_round_trip(kafka_bootstrap: str, topic: str) -> None:
    from confluent_kafka import Consumer, Producer  # noqa: PLC0415

    settings = KafkaSettings(bootstrap_servers=kafka_bootstrap, topic_trades=topic)
    publisher = MarketDataPublisher(
        Producer(settings.producer_config()), settings, IngestionMetrics(), producer_id="pytest"
    )
    publisher.bind_schemas({"trades": Trade})

    sent = [_trade(i) for i in range(50)]
    for trade in sent:
        assert publisher.publish("trades", trade).ok
    assert publisher.flush(30.0) == 0, "flush left messages undelivered: that is data loss"

    consumer = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap,
            "group.id": f"pytest-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    codec = AvroCodec.from_file("trade.avsc")
    received = []
    try:
        while len(received) < len(sent):
            message = consumer.poll(10.0)
            if message is None:
                break
            assert message.error() is None
            received.append(codec.decode(message.value()))
    finally:
        consumer.close()

    assert len(received) == len(sent)
    assert {row["trade_id"] for row in received} == {t.trade_id for t in sent}
    # The whole reason prices are decimal(38,18) rather than double.
    assert received[0]["price"] == Decimal("64250.100000000000000000")


def test_a_symbol_lands_on_exactly_one_partition(kafka_bootstrap: str, topic: str) -> None:
    """Ordering per instrument is a contract, not a happy accident."""
    from confluent_kafka import Consumer, Producer  # noqa: PLC0415

    settings = KafkaSettings(bootstrap_servers=kafka_bootstrap, topic_trades=topic)
    publisher = MarketDataPublisher(
        Producer(settings.producer_config()), settings, IngestionMetrics(), producer_id="pytest"
    )
    publisher.bind_schemas({"trades": Trade})

    for i in range(30):
        publisher.publish("trades", _trade(i, symbol="ETHUSDT"))
    publisher.flush(30.0)

    consumer = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap,
            "group.id": f"pytest-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    partitions = set()
    try:
        for _ in range(30):
            message = consumer.poll(10.0)
            if message is None:
                break
            partitions.add(message.partition())
    finally:
        consumer.close()

    assert len(partitions) == 1, f"ETHUSDT spread across partitions {partitions}"
