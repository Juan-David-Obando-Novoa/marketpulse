"""Translate venue payloads into contract records.

This module is the only place in the codebase that knows what Binance's field
names mean. ``e``, ``E``, ``T``, ``t``, ``m``, ``M``, ``u``, ``b``, ``B``,
``a``, ``A`` are single letters over the wire to save bandwidth, and letting
them leak past this boundary would put a decoding ring at every call site.

Everything here is a pure function: payload in, record out, no I/O and no
clock reads except an explicitly injected ``received_at``. That makes the
whole venue-specific surface testable against captured fixtures, which is the
only honest way to test an integration you do not control.
"""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from marketpulse.contracts.models import BookTicker, Kline, Trade
from marketpulse.utils.timeutil import epoch_millis_to_datetime, utc_now

__all__ = [
    "EXCHANGE",
    "NormalizationError",
    "SequenceTracker",
    "normalise_book_ticker",
    "normalise_rest_kline",
    "normalise_trade",
    "split_combined_stream",
    "stream_kind",
]

EXCHANGE = "binance"


class NormalizationError(ValueError):
    """A payload could not be mapped onto a contract record.

    Distinct from :class:`pydantic.ValidationError` on purpose: this one means
    *the shape is wrong* (a missing key, a non-numeric price), whereas a
    ValidationError means *the values are wrong* (a crossed book). Both end up
    in the dead-letter topic, but they are different upstream problems and the
    ``error_type`` label keeps them apart on the dashboard.
    """


def _decimal(payload: dict[str, Any], key: str, *, field: str) -> Decimal:
    """Parse a venue numeric string into Decimal, never through float.

    ``Decimal(float(x))`` would introduce representation error before the value
    ever reaches the contract; the venue sends decimal strings precisely so
    that it does not have to.
    """
    try:
        raw = payload[key]
    except KeyError as exc:
        raise NormalizationError(f"missing field {key!r} for {field}") from exc
    try:
        return Decimal(str(raw))
    except (InvalidOperation, TypeError) as exc:
        raise NormalizationError(f"field {field} ({key}={raw!r}) is not a decimal") from exc


def _millis(payload: dict[str, Any], key: str, *, field: str, default: int | None = None) -> int:
    raw = payload.get(key, default)
    if raw is None:
        raise NormalizationError(f"missing field {key!r} for {field}")
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise NormalizationError(f"field {field} ({key}={raw!r}) is not epoch millis") from exc


def split_combined_stream(payload: dict[str, Any]) -> tuple[str | None, dict[str, Any]]:
    """Unwrap the combined-stream envelope ``{"stream": ..., "data": {...}}``.

    Raw single streams are passed through unchanged, so the same normaliser
    works whether the client subscribed to one stream or twenty.
    """
    if "stream" in payload and isinstance(payload.get("data"), dict):
        return str(payload["stream"]), payload["data"]
    return None, payload


def stream_kind(stream_name: str | None, payload: dict[str, Any]) -> str:
    """Classify a message as ``trade``, ``bookTicker`` or ``unknown``.

    The stream name is authoritative when present; the ``e`` event-type field
    is the fallback. bookTicker messages historically arrive with neither, so
    the presence of the ``u`` update-id field is the last resort.
    """
    if stream_name and "@" in stream_name:
        return stream_name.split("@", 1)[1]
    event_type = payload.get("e")
    if isinstance(event_type, str):
        return event_type
    if {"u", "b", "a"} <= payload.keys():
        return "bookTicker"
    return "unknown"


def normalise_trade(
    payload: dict[str, Any],
    *,
    producer_id: str,
    received_at: datetime | None = None,
    keep_raw: bool = True,
) -> Trade:
    """Map a ``@trade`` payload onto :class:`Trade`.

    Venue field mapping::

        s -> symbol      t -> trade_id    p -> price      q -> quantity
        T -> trade_time  E -> event_time  m -> buyer_is_maker

    ``E`` falls back to ``T``: the venue omits the envelope timestamp on some
    historical replay paths, and a missing envelope time is not a reason to
    drop a real trade.
    """
    now = received_at or utc_now()
    try:
        symbol = str(payload["s"])
        trade_id = int(payload["t"])
    except (KeyError, TypeError, ValueError) as exc:
        raise NormalizationError(f"trade payload missing identity fields: {exc}") from exc

    trade_time_ms = _millis(payload, "T", field="trade_time")
    event_time_ms = _millis(payload, "E", field="event_time", default=trade_time_ms)

    return Trade(
        exchange=EXCHANGE,
        symbol=symbol,
        trade_id=trade_id,
        price=_decimal(payload, "p", field="price"),
        quantity=_decimal(payload, "q", field="quantity"),
        buyer_is_maker=bool(payload.get("m", False)),
        trade_time=epoch_millis_to_datetime(trade_time_ms),
        event_time=epoch_millis_to_datetime(event_time_ms),
        ingested_at=now,
        producer_id=producer_id,
        raw_payload=json.dumps(payload, separators=(",", ":")) if keep_raw else None,
    )


def normalise_book_ticker(
    payload: dict[str, Any],
    *,
    producer_id: str,
    received_at: datetime | None = None,
    keep_raw: bool = True,
) -> BookTicker:
    """Map a ``@bookTicker`` payload onto :class:`BookTicker`.

    Venue field mapping::

        s -> symbol   u -> update_id
        b -> bid_price  B -> bid_quantity  a -> ask_price  A -> ask_quantity

    The stream carries no event timestamp on the public endpoint, so local
    receipt time is used. That is recorded in the contract's ``doc`` rather
    than hidden here: a consumer computing latency from this field needs to
    know it is measuring our clock, not the venue's.
    """
    now = received_at or utc_now()
    try:
        symbol = str(payload["s"])
        update_id = int(payload["u"])
    except (KeyError, TypeError, ValueError) as exc:
        raise NormalizationError(f"book ticker payload missing identity fields: {exc}") from exc

    event_time_ms = payload.get("E") or payload.get("T")
    event_time = epoch_millis_to_datetime(int(event_time_ms)) if event_time_ms else now

    return BookTicker(
        exchange=EXCHANGE,
        symbol=symbol,
        update_id=update_id,
        bid_price=_decimal(payload, "b", field="bid_price"),
        bid_quantity=_decimal(payload, "B", field="bid_quantity"),
        ask_price=_decimal(payload, "a", field="ask_price"),
        ask_quantity=_decimal(payload, "A", field="ask_quantity"),
        event_time=event_time,
        ingested_at=now,
        producer_id=producer_id,
        raw_payload=json.dumps(payload, separators=(",", ":")) if keep_raw else None,
    )


#: Positional layout of the venue's REST kline response. It is a bare array,
#: which is efficient and completely opaque; naming the offsets once here is
#: the difference between readable code and row[9].
_KLINE_FIELDS = (
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
)


def normalise_rest_kline(
    row: list[Any],
    *,
    symbol: str,
    interval: str,
    producer_id: str,
    received_at: datetime | None = None,
    keep_raw: bool = True,
) -> Kline:
    """Map one row of ``GET /api/v3/klines`` onto :class:`Kline`.

    The response is an array of arrays with twelve positional entries and no
    names. A short response is rejected outright rather than padded, because a
    truncated row silently shifts every subsequent column and the OHLC
    invariant check would then fail somewhere far less informative.
    """
    if len(row) < len(_KLINE_FIELDS) - 1:
        raise NormalizationError(
            f"kline row for {symbol} has {len(row)} columns, expected at least "
            f"{len(_KLINE_FIELDS) - 1}"
        )
    fields = dict(zip(_KLINE_FIELDS, row, strict=False))

    def dec(name: str) -> Decimal:
        try:
            return Decimal(str(fields[name]))
        except (InvalidOperation, TypeError) as exc:
            raise NormalizationError(f"kline field {name}={fields[name]!r} is not decimal") from exc

    return Kline(
        exchange=EXCHANGE,
        symbol=symbol,
        interval=interval,
        open_time=epoch_millis_to_datetime(int(fields["open_time"])),
        close_time=epoch_millis_to_datetime(int(fields["close_time"])),
        open=dec("open"),
        high=dec("high"),
        low=dec("low"),
        close=dec("close"),
        volume=dec("volume"),
        quote_volume=dec("quote_volume"),
        trade_count=int(fields["trade_count"]),
        taker_buy_base_volume=dec("taker_buy_base_volume"),
        taker_buy_quote_volume=dec("taker_buy_quote_volume"),
        is_closed=True,
        ingested_at=received_at or utc_now(),
        producer_id=producer_id,
        raw_payload=json.dumps(row, separators=(",", ":")) if keep_raw else None,
    )


class SequenceTracker:
    """Detect gaps in a venue's monotonic per-symbol update sequence.

    The venue guarantees ``update_id`` increases by at least one per message
    for a symbol. A jump larger than one means messages were dropped somewhere
    between the matching engine and us -- and unlike a disconnect, this failure
    is completely silent. Counting it turns "the data looks a bit thin" into a
    number on a dashboard.

    Out-of-order and replayed ids (which a reconnect produces) are reported as
    duplicates rather than gaps, so a reconnect does not masquerade as loss.
    """

    __slots__ = ("_last",)

    def __init__(self) -> None:
        self._last: dict[str, int] = {}

    def observe(self, symbol: str, update_id: int) -> int:
        """Return the number of missed updates implied by ``update_id``.

        Returns ``0`` for the first observation of a symbol, for a contiguous
        increment, and for any non-advancing id.
        """
        previous = self._last.get(symbol)
        self._last[symbol] = max(update_id, previous) if previous is not None else update_id
        if previous is None or update_id <= previous:
            return 0
        return update_id - previous - 1

    def reset(self, symbol: str | None = None) -> None:
        """Forget state after a reconnect, where a gap is expected and uninformative."""
        if symbol is None:
            self._last.clear()
        else:
            self._last.pop(symbol, None)
