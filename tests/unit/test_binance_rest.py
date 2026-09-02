"""Backfill correctness: window arithmetic, paging, and rate-limit behaviour."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from marketpulse.config import BinanceSettings
from marketpulse.ingestion.binance_rest import (
    MAX_KLINES_PER_REQUEST,
    BinanceRestClient,
    KlineWindow,
    RateLimitError,
    interval_to_timedelta,
)
from tests.unit.fakes import FakeResponse, FakeSession
from tests.unit.test_normalizers import KLINE_ROW

UTC = timezone.utc
pytestmark = pytest.mark.unit

DAY = datetime(2026, 3, 14, tzinfo=UTC)


def _client(session: FakeSession) -> BinanceRestClient:
    return BinanceRestClient(BinanceSettings(rest_base_url="https://api.test"), session)


# --------------------------------------------------------------------------
# Window arithmetic
# --------------------------------------------------------------------------
def test_interval_lookup_rejects_an_unknown_code() -> None:
    with pytest.raises(ValueError, match="unsupported interval"):
        interval_to_timedelta("7s")


def test_window_must_be_forward() -> None:
    with pytest.raises(ValueError, match="must be after"):
        KlineWindow("BTCUSDT", "1m", DAY, DAY)


def test_expected_candle_count() -> None:
    window = KlineWindow("BTCUSDT", "1m", DAY, DAY + timedelta(days=1))
    assert window.expected_candles == 1_440


def test_chunks_never_exceed_one_page() -> None:
    """The venue silently truncates an oversized request; we never make one."""
    window = KlineWindow("BTCUSDT", "1m", DAY, DAY + timedelta(days=3))
    chunks = window.chunks()
    assert all(chunk.expected_candles <= MAX_KLINES_PER_REQUEST for chunk in chunks)
    assert sum(chunk.expected_candles for chunk in chunks) == window.expected_candles


def test_chunks_tile_without_overlap_or_gap() -> None:
    window = KlineWindow("BTCUSDT", "1m", DAY, DAY + timedelta(days=2))
    chunks = window.chunks()
    assert chunks[0].start == window.start
    assert chunks[-1].end == window.end
    for earlier, later in zip(chunks, chunks[1:], strict=False):
        assert earlier.end == later.start, "half-open windows must tile exactly"


def test_a_window_smaller_than_a_page_is_a_single_chunk() -> None:
    window = KlineWindow("BTCUSDT", "1h", DAY, DAY + timedelta(hours=5))
    assert len(window.chunks()) == 1


# --------------------------------------------------------------------------
# Fetching
# --------------------------------------------------------------------------
async def test_fetch_klines_normalises_every_row() -> None:
    session = FakeSession([FakeResponse(payload=[KLINE_ROW, KLINE_ROW])])
    klines = await _client(session).fetch_klines(
        KlineWindow("BTCUSDT", "1m", DAY, DAY + timedelta(minutes=2))
    )
    assert len(klines) == 2
    assert {k.symbol for k in klines} == {"BTCUSDT"}


async def test_end_time_is_exclusive_on_the_wire() -> None:
    """The venue's endTime is inclusive; without the -1ms, seams duplicate a candle."""
    session = FakeSession([FakeResponse(payload=[])])
    end = DAY + timedelta(minutes=10)
    await _client(session).fetch_klines(KlineWindow("BTCUSDT", "1m", DAY, end))

    _, params = session.requests[0]
    assert params["endTime"] == int(end.timestamp() * 1_000) - 1
    assert params["startTime"] == int(DAY.timestamp() * 1_000)


async def test_a_multi_page_window_issues_one_request_per_page() -> None:
    session = FakeSession([FakeResponse(payload=[]) for _ in range(3)])
    await _client(session).fetch_klines(
        KlineWindow("BTCUSDT", "1m", DAY, DAY + timedelta(days=2))
    )
    assert len(session.requests) == 3


async def test_used_weight_is_read_from_the_venue_header() -> None:
    """Counting requests locally is guesswork; the venue is the authority."""
    session = FakeSession(
        [FakeResponse(payload=[], headers={"X-MBX-USED-WEIGHT-1M": "137"})]
    )
    client = _client(session)
    await client.fetch_klines(KlineWindow("BTCUSDT", "1m", DAY, DAY + timedelta(minutes=1)))
    assert client.used_weight == 137


# --------------------------------------------------------------------------
# Rate limiting
# --------------------------------------------------------------------------
async def test_429_is_retried_after_the_requested_delay() -> None:
    session = FakeSession(
        [
            FakeResponse(status=429, headers={"Retry-After": "0"}),
            FakeResponse(payload=[KLINE_ROW]),
        ]
    )
    klines = await _client(session).fetch_klines(
        KlineWindow("BTCUSDT", "1m", DAY, DAY + timedelta(minutes=1))
    )
    assert len(klines) == 1
    assert len(session.requests) == 2


async def test_418_is_escalated_rather_than_retried() -> None:
    """418 means we already ignored a 429 and are banned. Retrying makes it worse."""
    session = FakeSession([FakeResponse(status=418, headers={"Retry-After": "120"})])
    with pytest.raises(RateLimitError) as excinfo:
        await _client(session).fetch_klines(
            KlineWindow("BTCUSDT", "1m", DAY, DAY + timedelta(minutes=1))
        )
    assert excinfo.value.banned is True
    assert excinfo.value.retry_after_seconds == 120


async def test_client_error_is_not_retried() -> None:
    session = FakeSession([FakeResponse(status=400, text="bad symbol")])
    with pytest.raises(RuntimeError, match="400"):
        await _client(session).fetch_klines(
            KlineWindow("BADSYM", "1m", DAY, DAY + timedelta(minutes=1))
        )
    assert len(session.requests) == 1


async def test_server_error_is_retried() -> None:
    session = FakeSession(
        [FakeResponse(status=503, text="unavailable"), FakeResponse(payload=[KLINE_ROW])]
    )
    klines = await _client(session).fetch_klines(
        KlineWindow("BTCUSDT", "1m", DAY, DAY + timedelta(minutes=1))
    )
    assert len(klines) == 1


# --------------------------------------------------------------------------
# Reference data and clocks
# --------------------------------------------------------------------------
async def test_exchange_info_encodes_the_symbols_filter_the_way_the_venue_wants() -> None:
    session = FakeSession([FakeResponse(payload={"symbols": []})])
    await _client(session).exchange_info(["BTCUSDT", "ETHUSDT"])
    _, params = session.requests[0]
    assert params["symbols"] == '["BTCUSDT","ETHUSDT"]'


async def test_clock_skew_is_measured_against_the_venue() -> None:
    """Every lag metric is computed against venue timestamps; skew biases them all."""
    session = FakeSession([FakeResponse(payload={"serverTime": 0})])
    skew = await _client(session).server_time_skew_ms()
    assert skew > 0
