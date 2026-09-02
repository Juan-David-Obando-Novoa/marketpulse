"""Normalisation is tested against captured payload shapes, not mocks.

An integration we do not control can only be tested honestly against what it
actually sends. These fixtures are the real shapes off the venue's public
combined stream and REST endpoint.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from marketpulse.ingestion.normalizers import (
    NormalizationError,
    SequenceTracker,
    normalise_book_ticker,
    normalise_rest_kline,
    normalise_trade,
    split_combined_stream,
    stream_kind,
)

pytestmark = pytest.mark.unit

TRADE_PAYLOAD: dict[str, Any] = {
    "e": "trade",
    "E": 1773500966535,
    "s": "BTCUSDT",
    "t": 4188923471,
    "p": "64250.10000000",
    "q": "0.01250000",
    "T": 1773500966500,
    "m": False,
    "M": True,
}

BOOK_TICKER_PAYLOAD: dict[str, Any] = {
    "u": 400900217,
    "s": "BTCUSDT",
    "b": "64250.00000000",
    "B": "31.21000000",
    "a": "64251.00000000",
    "A": "40.66000000",
}

KLINE_ROW: list[Any] = [
    1773500940000,
    "64200.00000000",
    "64300.00000000",
    "64150.00000000",
    "64250.00000000",
    "12.50000000",
    1773500999999,
    "803125.00000000",
    412,
    "6.25000000",
    "401562.50000000",
    "0",
]


# --------------------------------------------------------------------------
# Envelope handling
# --------------------------------------------------------------------------
def test_combined_stream_envelope_is_unwrapped() -> None:
    name, data = split_combined_stream({"stream": "btcusdt@trade", "data": TRADE_PAYLOAD})
    assert name == "btcusdt@trade"
    assert data is TRADE_PAYLOAD


def test_raw_single_stream_passes_through() -> None:
    name, data = split_combined_stream(TRADE_PAYLOAD)
    assert name is None
    assert data is TRADE_PAYLOAD


@pytest.mark.parametrize(
    ("stream_name", "payload", "expected"),
    [
        ("btcusdt@trade", {}, "trade"),
        ("ethusdt@bookTicker", {}, "bookTicker"),
        (None, {"e": "trade"}, "trade"),
        (None, BOOK_TICKER_PAYLOAD, "bookTicker"),
        (None, {"something": 1}, "unknown"),
    ],
)
def test_stream_kind_classification(
    stream_name: str | None, payload: dict[str, Any], expected: str
) -> None:
    assert stream_kind(stream_name, payload) == expected


# --------------------------------------------------------------------------
# Trades
# --------------------------------------------------------------------------
def test_trade_field_mapping() -> None:
    trade = normalise_trade(TRADE_PAYLOAD, producer_id="p1")
    assert trade.symbol == "BTCUSDT"
    assert trade.trade_id == 4188923471
    assert trade.price == Decimal("64250.10")
    assert trade.quantity == Decimal("0.0125")
    assert trade.buyer_is_maker is False
    assert trade.is_buyer_aggressor is True


def test_trade_distinguishes_matching_engine_time_from_envelope_time() -> None:
    """T is when the match happened; E is when the venue fanned it out."""
    trade = normalise_trade(TRADE_PAYLOAD, producer_id="p1")
    assert trade.event_time > trade.trade_time
    assert (trade.event_time - trade.trade_time).total_seconds() == pytest.approx(0.035)


def test_trade_event_time_falls_back_to_trade_time() -> None:
    payload = {k: v for k, v in TRADE_PAYLOAD.items() if k != "E"}
    trade = normalise_trade(payload, producer_id="p1")
    assert trade.event_time == trade.trade_time


def test_raw_payload_is_preserved_verbatim_by_default() -> None:
    trade = normalise_trade(TRADE_PAYLOAD, producer_id="p1")
    assert trade.raw_payload is not None
    assert json.loads(trade.raw_payload) == TRADE_PAYLOAD


def test_raw_payload_can_be_disabled_for_high_volume_streams() -> None:
    assert normalise_trade(TRADE_PAYLOAD, producer_id="p1", keep_raw=False).raw_payload is None


def test_price_never_round_trips_through_float() -> None:
    """Decimal(float('0.1')) is 0.1000000000000000055511151231257827."""
    payload = {**TRADE_PAYLOAD, "p": "0.10000000", "q": "3.00000000"}
    trade = normalise_trade(payload, producer_id="p1")
    assert trade.price * trade.quantity == Decimal("0.3")


@pytest.mark.parametrize("missing", ["s", "t", "T"])
def test_missing_identity_field_raises_normalization_error(missing: str) -> None:
    payload = {k: v for k, v in TRADE_PAYLOAD.items() if k != missing}
    with pytest.raises(NormalizationError):
        normalise_trade(payload, producer_id="p1")


def test_non_numeric_price_is_a_normalization_error_not_a_crash() -> None:
    with pytest.raises(NormalizationError, match="price"):
        normalise_trade({**TRADE_PAYLOAD, "p": "n/a"}, producer_id="p1")


# --------------------------------------------------------------------------
# Book ticker
# --------------------------------------------------------------------------
def test_book_ticker_field_mapping() -> None:
    ticker = normalise_book_ticker(BOOK_TICKER_PAYLOAD, producer_id="p1")
    assert ticker.update_id == 400900217
    assert ticker.bid_price == Decimal("64250")
    assert ticker.ask_quantity == Decimal("40.66")
    assert ticker.mid_price == Decimal("64250.5")


def test_book_ticker_without_venue_timestamp_uses_receipt_time() -> None:
    received = datetime(2026, 3, 14, tzinfo=timezone.utc)
    ticker = normalise_book_ticker(BOOK_TICKER_PAYLOAD, producer_id="p1", received_at=received)
    assert ticker.event_time == received == ticker.ingested_at


def test_crossed_quote_from_the_venue_is_rejected_not_normalised() -> None:
    """Bad upstream data must surface as an error the DLQ can capture."""
    payload = {**BOOK_TICKER_PAYLOAD, "b": "64252.00000000"}
    with pytest.raises(ValidationError, match="crossed book"):
        normalise_book_ticker(payload, producer_id="p1")


# --------------------------------------------------------------------------
# REST klines
# --------------------------------------------------------------------------
def test_kline_positional_mapping() -> None:
    kline = normalise_rest_kline(KLINE_ROW, symbol="BTCUSDT", interval="1m", producer_id="p1")
    assert kline.open == Decimal("64200")
    assert kline.close == Decimal("64250")
    assert kline.trade_count == 412
    assert kline.taker_buy_base_volume == Decimal("6.25")


def test_kline_window_is_inclusive_of_the_last_millisecond() -> None:
    kline = normalise_rest_kline(KLINE_ROW, symbol="BTCUSDT", interval="1m", producer_id="p1")
    assert (kline.close_time - kline.open_time).total_seconds() == pytest.approx(59.999)


def test_truncated_kline_row_is_rejected_rather_than_padded() -> None:
    """A short row shifts every later column; padding hides that until the OHLC check."""
    with pytest.raises(NormalizationError, match="columns"):
        normalise_rest_kline(KLINE_ROW[:5], symbol="BTCUSDT", interval="1m", producer_id="p1")


def test_column_order_mistake_is_caught_by_the_ohlc_invariant() -> None:
    swapped = list(KLINE_ROW)
    swapped[2], swapped[3] = swapped[3], swapped[2]  # high <-> low
    with pytest.raises(ValidationError, match="OHLC invariant"):
        normalise_rest_kline(swapped, symbol="BTCUSDT", interval="1m", producer_id="p1")


# --------------------------------------------------------------------------
# Sequence tracking
# --------------------------------------------------------------------------
def test_first_observation_has_nothing_to_compare_against() -> None:
    observation = SequenceTracker().observe("BTCUSDT", 1_000)
    assert observation.first_seen
    assert observation.span == 0
    assert not observation.regressed


def test_contiguous_ids_report_no_span() -> None:
    tracker = SequenceTracker()
    tracker.observe("BTCUSDT", 10)
    assert [tracker.observe("BTCUSDT", i).span for i in range(11, 15)] == [0, 0, 0, 0]


def test_a_jump_is_book_churn_and_not_an_error() -> None:
    """The bug this replaced: `u` counts book updates at every depth, so a jump
    between top-of-book messages is normal on a liquid instrument. It is
    reported as a span, and nothing treats it as a fault."""
    tracker = SequenceTracker()
    tracker.observe("BTCUSDT", 100)
    observation = tracker.observe("BTCUSDT", 130)
    assert observation.span == 29
    assert not observation.regressed


def test_a_non_advancing_id_is_a_regression() -> None:
    tracker = SequenceTracker()
    tracker.observe("BTCUSDT", 100)
    assert tracker.observe("BTCUSDT", 98).regressed
    assert tracker.observe("BTCUSDT", 100).regressed


def test_the_high_water_mark_never_moves_backwards() -> None:
    """Otherwise one replayed message makes every later one look out of order."""
    tracker = SequenceTracker()
    tracker.observe("BTCUSDT", 100)
    tracker.observe("BTCUSDT", 98)
    assert not tracker.observe("BTCUSDT", 101).regressed
    assert tracker.observe("BTCUSDT", 101).regressed, "101 is now the mark"


def test_symbols_are_tracked_independently() -> None:
    tracker = SequenceTracker()
    tracker.observe("BTCUSDT", 500)
    assert tracker.observe("ETHUSDT", 900_000).first_seen


def test_reset_clears_state_for_a_reconnect() -> None:
    tracker = SequenceTracker()
    tracker.observe("BTCUSDT", 100)
    tracker.reset()
    assert tracker.observe("BTCUSDT", 5_000).first_seen
