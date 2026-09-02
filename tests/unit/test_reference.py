"""Instrument reference extraction.

The venue returns filters as a heterogeneous list keyed by `filterType` rather
than as named fields, which is the single most awkward shape in its API and the
one most likely to be handled by index somewhere.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from marketpulse.ingestion.reference import extract_filters, observations_from_exchange_info

UTC = timezone.utc
pytestmark = pytest.mark.unit

EXCHANGE_INFO = {
    "timezone": "UTC",
    "symbols": [
        {
            "symbol": "BTCUSDT",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.01000000", "minPrice": "0.01"},
                {"filterType": "LOT_SIZE", "stepSize": "0.00001000", "minQty": "0.00001"},
                {"filterType": "NOTIONAL", "minNotional": "5.00000000"},
            ],
        },
        {
            "symbol": "ETHUSDT",
            "status": "TRADING",
            "baseAsset": "ETH",
            "quoteAsset": "USDT",
            # Older venue responses use MIN_NOTIONAL rather than NOTIONAL.
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.01000000"},
                {"filterType": "MIN_NOTIONAL", "minNotional": "10.00000000"},
            ],
        },
        {
            "symbol": "HALTEDUSDT",
            "status": "BREAK",
            "baseAsset": "HALTED",
            "quoteAsset": "USDT",
            "filters": [],
        },
    ],
}


def test_filters_are_looked_up_by_type_not_by_position() -> None:
    """Filter order is not part of the venue's contract; indexing into it is a bug."""
    reordered = {
        "filters": [
            {"filterType": "NOTIONAL", "minNotional": "5"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001"},
            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
        ]
    }
    filters = extract_filters(reordered)
    assert filters["tick_size"] == Decimal("0.01")
    assert filters["step_size"] == Decimal("0.001")


def test_both_notional_filter_names_are_accepted() -> None:
    observations = {o.symbol: o for o in observations_from_exchange_info(EXCHANGE_INFO)}
    assert observations["BTCUSDT"].min_notional == Decimal("5")
    assert observations["ETHUSDT"].min_notional == Decimal("10")


def test_missing_filter_is_none_not_a_default() -> None:
    """A fabricated tick size makes the downstream grid assertion pass wrongly."""
    filters = extract_filters({"filters": []})
    assert filters == {"tick_size": None, "step_size": None, "min_notional": None}


def test_unparseable_filter_value_degrades_to_none() -> None:
    filters = extract_filters(
        {"filters": [{"filterType": "PRICE_FILTER", "tickSize": "not-a-number"}]}
    )
    assert filters["tick_size"] is None


def test_symbols_filter_restricts_the_result() -> None:
    observations = observations_from_exchange_info(EXCHANGE_INFO, symbols=["btcusdt"])
    assert [o.symbol for o in observations] == ["BTCUSDT"]


def test_halted_instruments_are_still_observed() -> None:
    """A halt is exactly the change the SCD2 dimension exists to record."""
    observations = {o.symbol: o for o in observations_from_exchange_info(EXCHANGE_INFO)}
    assert observations["HALTEDUSDT"].status == "BREAK"


def test_observed_at_is_our_clock_and_is_shared_across_the_batch() -> None:
    """One request, one observation time. The venue publishes no change timestamp."""
    moment = datetime(2026, 3, 14, 12, 0, tzinfo=UTC)
    observations = observations_from_exchange_info(EXCHANGE_INFO, observed_at=moment)
    assert {o.observed_at for o in observations} == {moment}


def test_row_shape_matches_the_bronze_table() -> None:
    observation = observations_from_exchange_info(EXCHANGE_INFO, symbols=["BTCUSDT"])[0]
    assert set(observation.as_row()) == {
        "symbol",
        "base_asset",
        "quote_asset",
        "status",
        "observed_at",
        "tick_size",
        "step_size",
        "min_notional",
    }
