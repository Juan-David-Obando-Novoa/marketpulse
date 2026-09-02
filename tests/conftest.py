"""Shared fixtures.

Everything here is hermetic. Fixtures that need a broker or a catalog live in
``tests/integration/conftest.py`` and are gated behind the ``integration``
marker so that ``pytest -m unit`` stays runnable on a laptop with nothing
installed.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from marketpulse.config import AppSettings, BinanceSettings, KafkaSettings
from marketpulse.contracts.models import BookTicker, Kline, Trade

UTC = timezone.utc


@pytest.fixture
def settings() -> AppSettings:
    """Deterministic settings, independent of the ambient environment."""
    return AppSettings(
        kafka=KafkaSettings(bootstrap_servers="broker:9092"),
        binance=BinanceSettings(symbols=["BTCUSDT", "ETHUSDT"], streams=["trade"]),
    )


@pytest.fixture
def moment() -> datetime:
    return datetime(2026, 3, 14, 15, 9, 26, 535000, tzinfo=UTC)


@pytest.fixture
def trade(moment: datetime) -> Trade:
    return Trade(
        symbol="BTCUSDT",
        trade_id=1_234_567,
        price=Decimal("64250.10"),
        quantity=Decimal("0.0125"),
        buyer_is_maker=False,
        trade_time=moment,
        event_time=moment + timedelta(milliseconds=3),
        ingested_at=moment + timedelta(milliseconds=41),
        producer_id="test-producer",
    )


@pytest.fixture
def book_ticker(moment: datetime) -> BookTicker:
    return BookTicker(
        symbol="BTCUSDT",
        update_id=98_765,
        bid_price=Decimal("64250.00"),
        bid_quantity=Decimal("1.5"),
        ask_price=Decimal("64251.00"),
        ask_quantity=Decimal("2.25"),
        event_time=moment,
        ingested_at=moment,
        producer_id="test-producer",
    )


@pytest.fixture
def kline(moment: datetime) -> Kline:
    open_time = moment.replace(second=0, microsecond=0)
    return Kline(
        symbol="BTCUSDT",
        interval="1m",
        open_time=open_time,
        close_time=open_time + timedelta(minutes=1, milliseconds=-1),
        open=Decimal("64200"),
        high=Decimal("64300"),
        low=Decimal("64150"),
        close=Decimal("64250"),
        volume=Decimal("12.5"),
        quote_volume=Decimal("803125"),
        trade_count=412,
        taker_buy_base_volume=Decimal("6.25"),
        taker_buy_quote_volume=Decimal("401562.5"),
        ingested_at=moment,
        producer_id="test-producer",
    )
