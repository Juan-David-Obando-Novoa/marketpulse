"""The feed's state machine, driven by a scripted socket.

Nothing here touches a network. The value of these tests is that they exercise
the paths that only occur in production -- a silent socket, a malformed frame
arriving mid-stream, a venue sequence gap, a reconnect -- deterministically and
in milliseconds.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from marketpulse.config import BinanceSettings, KafkaSettings
from marketpulse.contracts.models import BookTicker, Trade
from marketpulse.ingestion.binance_ws import (
    MAX_STREAMS_PER_CONNECTION,
    BinanceMarketDataFeed,
    build_stream_url,
)
from marketpulse.ingestion.publisher import MarketDataPublisher
from marketpulse.observability import IngestionMetrics
from tests.unit.fakes import FakeProducer, FakeSocket, ScriptedConnect
from tests.unit.test_normalizers import BOOK_TICKER_PAYLOAD, TRADE_PAYLOAD

pytestmark = pytest.mark.unit


def _frame(stream: str, payload: dict[str, object]) -> str:
    return json.dumps({"stream": stream, "data": payload})


def _build(
    connect: ScriptedConnect, **overrides: object
) -> tuple[BinanceMarketDataFeed, FakeProducer, IngestionMetrics]:
    settings = BinanceSettings(
        symbols=["BTCUSDT"],
        streams=["trade", "bookTicker"],
        idle_timeout_seconds=0.05,
        reconnect_initial_backoff_seconds=0.01,
        reconnect_max_backoff_seconds=0.02,
        **overrides,  # type: ignore[arg-type]
    )
    producer = FakeProducer()
    metrics = IngestionMetrics()
    publisher = MarketDataPublisher(producer, KafkaSettings(), metrics, producer_id="test")
    publisher.bind_schemas({"trades": Trade, "book_ticker": BookTicker})
    feed = BinanceMarketDataFeed(settings, publisher, metrics, producer_id="test", connect=connect)
    return feed, producer, metrics


def _counter(metrics: IngestionMetrics, name: str, **labels: str) -> float:
    return metrics.registry.get_sample_value(name, labels) or 0.0


async def _run_briefly(feed: BinanceMarketDataFeed, *, settle: float = 0.15) -> None:
    """Run the feed's real loop, then ask it to stop.

    Tests must not pre-set the stop flag: the loop checks it before the first
    connection, so doing that would assert on a feed that never connected --
    which is precisely the bug this helper was written after hitting.
    """
    task = asyncio.create_task(feed.run())
    await asyncio.sleep(settle)
    feed.request_stop()
    await asyncio.wait_for(task, timeout=2.0)


# --------------------------------------------------------------------------
# Subscription URL
# --------------------------------------------------------------------------
def test_stream_url_joins_streams() -> None:
    url = build_stream_url("wss://x/stream", ["btcusdt@trade", "ethusdt@trade"])
    assert url == "wss://x/stream?streams=btcusdt@trade/ethusdt@trade"


def test_empty_subscription_is_rejected() -> None:
    with pytest.raises(ValueError, match="at least one stream"):
        build_stream_url("wss://x/stream", [])


def test_oversized_subscription_fails_loudly_rather_than_being_truncated() -> None:
    """The venue truncates silently, which presents as 'one symbol has no data'."""
    streams = [f"sym{i}usdt@trade" for i in range(MAX_STREAMS_PER_CONNECTION + 1)]
    with pytest.raises(ValueError, match="exceeds"):
        build_stream_url("wss://x/stream", streams)


# --------------------------------------------------------------------------
# Happy path
# --------------------------------------------------------------------------
async def test_trades_and_quotes_are_published_to_their_topics() -> None:
    connect = ScriptedConnect(
        [
            FakeSocket(
                [
                    _frame("btcusdt@trade", TRADE_PAYLOAD),
                    _frame("btcusdt@bookTicker", BOOK_TICKER_PAYLOAD),
                ]
            )
        ]
    )
    feed, producer, metrics = _build(connect)

    await _run_briefly(feed)

    assert producer.topics() == ["md.trades.v1", "md.book_ticker.v1"]
    assert feed.messages_seen == 2
    assert (
        _counter(metrics, "marketpulse_messages_received_total", source="binance", stream="trade")
        == 1.0
    )


async def test_control_frames_are_ignored_not_dead_lettered() -> None:
    connect = ScriptedConnect([FakeSocket([json.dumps({"result": None, "id": 1})])])
    feed, producer, _ = _build(connect)

    await _run_briefly(feed)

    assert producer.messages == []


# --------------------------------------------------------------------------
# Failure isolation
# --------------------------------------------------------------------------
async def test_malformed_json_is_dead_lettered_and_the_stream_continues() -> None:
    """One bad frame must not take down the other nineteen symbols."""
    connect = ScriptedConnect([FakeSocket(["{not json", _frame("btcusdt@trade", TRADE_PAYLOAD)])])
    feed, producer, _ = _build(connect)

    await _run_briefly(feed)

    assert producer.topics() == ["md.dead_letter.v1", "md.trades.v1"]


async def test_crossed_quote_is_dead_lettered_with_its_origin_stream() -> None:
    crossed = {**BOOK_TICKER_PAYLOAD, "b": "99999.00000000"}
    connect = ScriptedConnect([FakeSocket([_frame("btcusdt@bookTicker", crossed)])])
    feed, producer, metrics = _build(connect)

    await _run_briefly(feed)

    assert producer.topics() == ["md.dead_letter.v1"]
    assert (
        _counter(
            metrics,
            "marketpulse_dead_letters_total",
            origin_topic="md.book_ticker.v1",
            error_type="ValidationError",
        )
        == 1.0
    )


async def test_venue_sequence_gap_is_counted() -> None:
    frames = [
        _frame("btcusdt@bookTicker", {**BOOK_TICKER_PAYLOAD, "u": 100}),
        _frame("btcusdt@bookTicker", {**BOOK_TICKER_PAYLOAD, "u": 104}),
    ]
    connect = ScriptedConnect([FakeSocket(frames)])
    feed, _, metrics = _build(connect)

    await _run_briefly(feed)

    assert (
        _counter(metrics, "marketpulse_sequence_gaps_total", source="binance", symbol="BTCUSDT")
        == 3.0
    )


# --------------------------------------------------------------------------
# Reconnect state machine
# --------------------------------------------------------------------------
async def test_silent_socket_triggers_a_reconnect() -> None:
    """The failure TCP will not surface: open, connected, and saying nothing."""
    connect = ScriptedConnect(
        [
            FakeSocket([_frame("btcusdt@trade", TRADE_PAYLOAD)], after="hang"),
            FakeSocket([_frame("btcusdt@trade", TRADE_PAYLOAD)]),
        ]
    )
    feed, producer, metrics = _build(connect)

    task = asyncio.create_task(feed.run())
    await asyncio.sleep(0.25)
    feed.request_stop()
    await asyncio.wait_for(task, timeout=2.0)

    assert connect.attempts >= 2, "the idle watchdog should have forced a reconnect"
    assert (
        _counter(
            metrics, "marketpulse_feed_reconnects_total", source="binance", reason="ConnectionError"
        )
        >= 1.0
    )
    assert len(producer.messages) >= 2


async def test_connection_error_is_retried_with_backoff() -> None:
    connect = ScriptedConnect(
        [
            ConnectionRefusedError("venue unreachable"),
            FakeSocket([_frame("btcusdt@trade", TRADE_PAYLOAD)]),
        ]
    )
    feed, producer, _metrics = _build(connect)

    task = asyncio.create_task(feed.run())
    await asyncio.sleep(0.2)
    feed.request_stop()
    await asyncio.wait_for(task, timeout=2.0)

    assert connect.attempts >= 2
    assert producer.topics()[0] == "md.trades.v1"


async def test_sequence_state_is_reset_across_a_reconnect() -> None:
    """A replayed id after reconnect is recovery, not loss; counting it inverts the metric."""
    connect = ScriptedConnect(
        [
            FakeSocket([_frame("btcusdt@bookTicker", {**BOOK_TICKER_PAYLOAD, "u": 500})]),
            FakeSocket([_frame("btcusdt@bookTicker", {**BOOK_TICKER_PAYLOAD, "u": 9_000})]),
        ]
    )
    feed, _, metrics = _build(connect)

    task = asyncio.create_task(feed.run())
    await asyncio.sleep(0.15)
    feed.request_stop()
    await asyncio.wait_for(task, timeout=2.0)

    assert (
        _counter(metrics, "marketpulse_sequence_gaps_total", source="binance", symbol="BTCUSDT")
        == 0.0
    )


async def test_connected_gauge_returns_to_zero_after_the_loop_exits() -> None:
    connect = ScriptedConnect([FakeSocket([_frame("btcusdt@trade", TRADE_PAYLOAD)])])
    feed, _, metrics = _build(connect)

    await _run_briefly(feed)

    assert (
        metrics.registry.get_sample_value("marketpulse_feed_connected", {"source": "binance"})
        == 0.0
    )
