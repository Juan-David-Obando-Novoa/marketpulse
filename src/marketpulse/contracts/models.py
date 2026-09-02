"""Pydantic mirrors of the Avro contracts.

These models are the validation boundary at the edge of the producer: a
payload that does not satisfy one of them never reaches Kafka. They are
deliberately *not* generated from the ``.avsc`` files, and the round-trip is
asserted in ``tests/unit/test_contracts.py`` instead. Generated models tend to
be anaemic; these carry the natural key, the partition key and the Avro
projection as behaviour, which is where the interesting invariants live.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from marketpulse.utils.timeutil import datetime_to_epoch_millis, utc_now

__all__ = [
    "BookTicker",
    "DeadLetter",
    "FxRate",
    "Kline",
    "MarketDataRecord",
    "Trade",
]

#: Scale of the decimal logical type in the Avro contracts. Quantising to this
#: scale at the edge means every engine downstream sees identical bytes.
DECIMAL_SCALE = Decimal("1E-18")


def _quantise(value: Decimal) -> Decimal:
    """Normalise a decimal to the contract's scale without losing precision."""
    return value.quantize(DECIMAL_SCALE)


class MarketDataRecord(BaseModel):
    """Behaviour shared by every record we publish.

    Subclasses declare two things: the natural key that makes a record
    identifiable across replays (ADR-0007), and the Kafka partition key.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        str_strip_whitespace=True,
        ser_json_timedelta="float",
    )

    #: Avro schema file that backs this model, relative to ``contracts/schemas``.
    avro_schema_file: ClassVar[str]

    schema_version: int = 1
    ingested_at: datetime = Field(default_factory=utc_now)
    producer_id: str = "unknown"
    raw_payload: str | None = None

    @property
    def natural_key(self) -> tuple[Any, ...]:  # pragma: no cover - abstract
        raise NotImplementedError

    @property
    def partition_key(self) -> str:  # pragma: no cover - abstract
        raise NotImplementedError

    def to_avro_dict(self) -> dict[str, Any]:
        """Project to the dict shape fastavro expects for the Avro schema.

        Datetimes are handed over as aware ``datetime`` objects because
        fastavro encodes ``timestamp-millis`` itself; doing the arithmetic here
        as well would double-convert.
        """
        return self.model_dump(mode="python")

    def lag_millis(self) -> int:
        """Observed producer-side lag: ingestion time minus venue event time."""
        event_time = getattr(self, "event_time", None)
        if event_time is None:
            return 0
        return datetime_to_epoch_millis(self.ingested_at) - datetime_to_epoch_millis(event_time)


class Trade(MarketDataRecord):
    """One executed trade from the public tape."""

    avro_schema_file: ClassVar[str] = "trade.avsc"

    exchange: str = "binance"
    symbol: str
    trade_id: int = Field(ge=0)
    price: Decimal = Field(gt=0)
    quantity: Decimal = Field(gt=0)
    quote_quantity: Decimal | None = None
    buyer_is_maker: bool
    trade_time: datetime
    event_time: datetime

    @field_validator("symbol", "exchange")
    @classmethod
    def _upper(cls, value: str) -> str:
        return value.upper() if value.isascii() else value

    @field_validator("price", "quantity", "quote_quantity")
    @classmethod
    def _scale(cls, value: Decimal | None) -> Decimal | None:
        return None if value is None else _quantise(value)

    @model_validator(mode="after")
    def _derive_quote_quantity(self) -> Trade:
        """Compute notional at the edge so every engine agrees on rounding."""
        if self.quote_quantity is None:
            object.__setattr__(self, "quote_quantity", _quantise(self.price * self.quantity))
        return self

    @property
    def natural_key(self) -> tuple[str, str, int]:
        return (self.exchange, self.symbol, self.trade_id)

    @property
    def partition_key(self) -> str:
        return self.symbol

    @property
    def is_buyer_aggressor(self) -> bool:
        """True when the taker was buying, i.e. the trade lifted the offer.

        ``buyer_is_maker`` is the venue's field and reads backwards to almost
        everyone; naming the useful direction once here keeps the sign
        convention out of four different SQL models.
        """
        return not self.buyer_is_maker


class BookTicker(MarketDataRecord):
    """Top-of-book snapshot emitted on every change."""

    avro_schema_file: ClassVar[str] = "book_ticker.avsc"

    exchange: str = "binance"
    symbol: str
    update_id: int = Field(ge=0)
    bid_price: Decimal = Field(gt=0)
    bid_quantity: Decimal = Field(ge=0)
    ask_price: Decimal = Field(gt=0)
    ask_quantity: Decimal = Field(ge=0)
    event_time: datetime = Field(default_factory=utc_now)

    @field_validator("symbol", "exchange")
    @classmethod
    def _upper(cls, value: str) -> str:
        return value.upper()

    @field_validator("bid_price", "bid_quantity", "ask_price", "ask_quantity")
    @classmethod
    def _scale(cls, value: Decimal) -> Decimal:
        return _quantise(value)

    @model_validator(mode="after")
    def _reject_crossed_book(self) -> BookTicker:
        """A crossed book (bid >= ask) is an arbitrage or, far more likely, bad data.

        Rejecting it here sends the message to the dead-letter topic where it
        is counted and inspectable, instead of letting a negative spread
        propagate into a volatility metric.
        """
        if self.bid_price >= self.ask_price:
            raise ValueError(
                f"crossed book for {self.symbol}: bid {self.bid_price} >= ask {self.ask_price}"
            )
        return self

    @property
    def natural_key(self) -> tuple[str, str, int]:
        return (self.exchange, self.symbol, self.update_id)

    @property
    def partition_key(self) -> str:
        return self.symbol

    @property
    def mid_price(self) -> Decimal:
        return _quantise((self.bid_price + self.ask_price) / 2)

    @property
    def spread_bps(self) -> Decimal:
        """Quoted spread in basis points of the mid. The standard liquidity unit."""
        return _quantise((self.ask_price - self.bid_price) / self.mid_price * Decimal(10_000))


class Kline(MarketDataRecord):
    """Venue-published OHLCV candle."""

    avro_schema_file: ClassVar[str] = "kline.avsc"

    exchange: str = "binance"
    symbol: str
    interval: str
    open_time: datetime
    close_time: datetime
    open: Decimal = Field(gt=0)
    high: Decimal = Field(gt=0)
    low: Decimal = Field(gt=0)
    close: Decimal = Field(gt=0)
    volume: Decimal = Field(ge=0)
    quote_volume: Decimal = Field(ge=0)
    trade_count: int = Field(ge=0)
    taker_buy_base_volume: Decimal = Field(ge=0)
    taker_buy_quote_volume: Decimal = Field(ge=0)
    is_closed: bool = True

    @field_validator("symbol", "exchange")
    @classmethod
    def _upper(cls, value: str) -> str:
        return value.upper()

    @model_validator(mode="after")
    def _check_ohlc_invariants(self) -> Kline:
        """high >= max(open, close) >= min(open, close) >= low, and a sane window.

        These are the cheapest possible integrity tests and they catch the two
        failure modes that actually happen: a column-order mistake in a REST
        response, and a truncated payload.
        """
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close):
            raise ValueError(
                f"OHLC invariant violated for {self.symbol} at {self.open_time.isoformat()}: "
                f"o={self.open} h={self.high} l={self.low} c={self.close}"
            )
        if self.close_time <= self.open_time:
            raise ValueError("close_time must be strictly after open_time")
        if self.taker_buy_base_volume > self.volume:
            raise ValueError("taker buy volume cannot exceed total volume")
        return self

    @property
    def natural_key(self) -> tuple[str, str, str, int]:
        return (
            self.exchange,
            self.symbol,
            self.interval,
            datetime_to_epoch_millis(self.open_time),
        )

    @property
    def partition_key(self) -> str:
        return f"{self.symbol}:{self.interval}"


class FxRate(MarketDataRecord):
    """Official reference rate with an explicit validity interval."""

    avro_schema_file: ClassVar[str] = "fx_rate.avsc"

    source: str
    base_currency: str = Field(min_length=3, max_length=3)
    quote_currency: str = Field(min_length=3, max_length=3)
    rate: Decimal = Field(gt=0)
    valid_from: datetime
    valid_to: datetime

    @field_validator("base_currency", "quote_currency")
    @classmethod
    def _iso4217(cls, value: str) -> str:
        upper = value.upper()
        if not upper.isalpha():
            raise ValueError(f"currency code must be alphabetic, got {value!r}")
        return upper

    @model_validator(mode="after")
    def _check_interval(self) -> FxRate:
        if self.valid_to <= self.valid_from:
            raise ValueError("valid_to must be strictly after valid_from")
        return self

    @property
    def natural_key(self) -> tuple[str, str, str, int]:
        return (
            self.source,
            self.base_currency,
            self.quote_currency,
            datetime_to_epoch_millis(self.valid_from),
        )

    @property
    def partition_key(self) -> str:
        return f"{self.base_currency}{self.quote_currency}"


class DeadLetter(BaseModel):
    """Envelope for a message that failed validation or serialisation."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    avro_schema_file: ClassVar[str] = "dead_letter.avsc"

    schema_version: int = 1
    origin_topic: str
    origin_stream: str | None = None
    error_type: str
    error_message: str
    raw_payload: str
    failed_at: datetime = Field(default_factory=utc_now)
    producer_id: str = "unknown"

    @classmethod
    def from_exception(
        cls,
        exc: Exception,
        *,
        origin_topic: str,
        raw_payload: str,
        origin_stream: str | None = None,
        producer_id: str = "unknown",
    ) -> DeadLetter:
        """Build an envelope from a caught exception.

        ``error_type`` is the exception class name specifically because it is
        low cardinality and therefore safe as a Prometheus label and as a
        Grafana ``group by``; the free-text message is not.
        """
        return cls(
            origin_topic=origin_topic,
            origin_stream=origin_stream,
            error_type=type(exc).__name__,
            error_message=str(exc)[:4_000],
            raw_payload=raw_payload[:64_000],
            producer_id=producer_id,
        )

    def to_avro_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="python")
