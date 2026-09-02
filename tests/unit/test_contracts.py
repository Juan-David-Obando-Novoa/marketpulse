"""The contract tests.

Two things are asserted here that nothing else in the codebase can assert:
that the hand-written pydantic models and the hand-written Avro schemas have
not drifted apart, and that the validators reject the specific bad data that
market feeds actually produce.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from marketpulse.contracts.models import BookTicker, DeadLetter, FxRate, Kline, Trade
from marketpulse.contracts.registry import (
    AvroCodec,
    is_framed,
    list_schema_files,
    load_schema,
    schema_id_of,
    strip_confluent_header,
    subject_for,
)

UTC = timezone.utc
pytestmark = pytest.mark.unit

MODELS = [Trade, BookTicker, Kline, FxRate, DeadLetter]


# --------------------------------------------------------------------------
# Model <-> schema drift
# --------------------------------------------------------------------------
@pytest.mark.parametrize("model", MODELS, ids=lambda m: m.__name__)
def test_every_model_points_at_a_schema_that_exists(model: type) -> None:
    assert model.avro_schema_file in list_schema_files()


@pytest.mark.parametrize("model", MODELS, ids=lambda m: m.__name__)
def test_model_fields_match_avro_fields_exactly(model: type) -> None:
    """Drift between the pydantic model and the .avsc is the whole risk here.

    Both are hand-written on purpose (see contracts/models.py); this test is
    what makes that safe.
    """
    schema = load_schema(model.avro_schema_file)
    avro_fields = {field["name"] for field in schema["fields"]}
    model_fields = set(model.model_fields)
    assert model_fields == avro_fields, (
        f"{model.__name__} drifted from {model.avro_schema_file}: "
        f"only in model={sorted(model_fields - avro_fields)}, "
        f"only in avro={sorted(avro_fields - model_fields)}"
    )


@pytest.mark.parametrize("filename", list_schema_files())
def test_every_avro_field_is_documented(filename: str) -> None:
    """A contract without field documentation is a contract nobody can consume."""
    schema = load_schema(filename)
    assert schema.get("doc"), f"{filename} has no record-level doc"


@pytest.mark.parametrize("filename", list_schema_files())
def test_optional_fields_declare_defaults(filename: str) -> None:
    """BACKWARD compatibility (ADR-0006) requires nullable fields to default."""
    for field in load_schema(filename)["fields"]:
        field_type = field["type"]
        is_union_with_null = isinstance(field_type, list) and "null" in field_type
        if is_union_with_null:
            assert "default" in field, f"{filename}:{field['name']} is nullable without a default"


# --------------------------------------------------------------------------
# Round-trip through the wire format
# --------------------------------------------------------------------------
def test_trade_round_trips_through_avro(trade: Trade) -> None:
    codec = AvroCodec.from_file(Trade.avro_schema_file, schema_id=101)
    payload = codec.encode(trade.to_avro_dict())
    decoded = codec.decode(payload)
    assert decoded["symbol"] == trade.symbol
    assert decoded["trade_id"] == trade.trade_id
    assert decoded["price"] == trade.price
    assert decoded["buyer_is_maker"] is trade.buyer_is_maker


def test_decimal_precision_survives_the_wire(trade: Trade) -> None:
    """The reason prices are decimal and not double. 0.1 + 0.2 has no place here."""
    codec = AvroCodec.from_file(Trade.avro_schema_file)
    awkward = trade.model_copy(update={"price": Decimal("0.1"), "quantity": Decimal("0.2")})
    decoded = codec.decode(codec.encode(awkward.to_avro_dict()))
    assert decoded["price"] * decoded["quantity"] == Decimal("0.02")


def test_framing_is_detectable_and_strippable(trade: Trade) -> None:
    codec = AvroCodec.from_file(Trade.avro_schema_file, schema_id=7)
    framed = codec.encode(trade.to_avro_dict())
    assert is_framed(framed)
    assert schema_id_of(framed) == 7
    bare = AvroCodec.from_file(Trade.avro_schema_file).encode(trade.to_avro_dict())
    assert strip_confluent_header(framed) == bare


def test_bare_payload_is_not_mistaken_for_framed(trade: Trade) -> None:
    with pytest.raises(ValueError, match="not in Confluent wire format"):
        strip_confluent_header(b"")


def test_subject_naming_follows_topic_name_strategy() -> None:
    assert subject_for("md.trades.v1") == "md.trades.v1-value"
    assert subject_for("md.trades.v1", is_key=True) == "md.trades.v1-key"


# --------------------------------------------------------------------------
# Validators: the bad data that market feeds actually emit
# --------------------------------------------------------------------------
def test_quote_quantity_is_derived_when_absent(trade: Trade) -> None:
    assert trade.quote_quantity == trade.price * trade.quantity


def test_buyer_aggressor_is_the_inverse_of_the_venue_flag(trade: Trade) -> None:
    assert trade.is_buyer_aggressor is not trade.buyer_is_maker


@pytest.mark.parametrize("bad_price", [Decimal("0"), Decimal("-1")])
def test_non_positive_price_is_rejected(trade: Trade, bad_price: Decimal) -> None:
    with pytest.raises(ValidationError):
        Trade.model_validate({**trade.model_dump(), "price": bad_price})


def test_models_are_frozen(trade: Trade) -> None:
    """Records are immutable: a mutated event is a different event."""
    with pytest.raises(ValidationError):
        trade.symbol = "ETHUSDT"  # type: ignore[misc]


def test_unknown_fields_are_rejected_not_silently_dropped(trade: Trade) -> None:
    """Silently dropping an unexpected upstream field is how drift goes unnoticed."""
    with pytest.raises(ValidationError):
        Trade.model_validate({**trade.model_dump(), "surprise_field": 1})


def test_crossed_book_is_rejected(book_ticker: BookTicker) -> None:
    """A negative spread is bad data, not arbitrage. It must not reach a metric."""
    payload = book_ticker.model_dump()
    payload["bid_price"] = payload["ask_price"] + Decimal("1")
    with pytest.raises(ValidationError, match="crossed book"):
        BookTicker.model_validate(payload)


def test_locked_book_is_also_rejected(book_ticker: BookTicker) -> None:
    payload = book_ticker.model_dump()
    payload["bid_price"] = payload["ask_price"]
    with pytest.raises(ValidationError, match="crossed book"):
        BookTicker.model_validate(payload)


def test_spread_in_basis_points(book_ticker: BookTicker) -> None:
    # 1.00 wide on a 64250.50 mid -> 0.1556 bps
    assert book_ticker.spread_bps == pytest.approx(Decimal("0.1556"), abs=Decimal("0.0001"))


def test_kline_rejects_high_below_close(kline: Kline) -> None:
    payload = kline.model_dump()
    payload["high"] = payload["close"] - Decimal("1")
    with pytest.raises(ValidationError, match="OHLC invariant"):
        Kline.model_validate(payload)


def test_kline_rejects_low_above_open(kline: Kline) -> None:
    payload = kline.model_dump()
    payload["low"] = payload["open"] + Decimal("1")
    with pytest.raises(ValidationError, match="OHLC invariant"):
        Kline.model_validate(payload)


def test_kline_rejects_taker_volume_exceeding_total(kline: Kline) -> None:
    payload = kline.model_dump()
    payload["taker_buy_base_volume"] = payload["volume"] + Decimal("1")
    with pytest.raises(ValidationError, match="taker buy volume"):
        Kline.model_validate(payload)


def test_fx_rate_requires_a_forward_interval() -> None:
    now = datetime(2026, 3, 14, tzinfo=UTC)
    with pytest.raises(ValidationError, match="valid_to"):
        FxRate(
            source="banrep-trm",
            base_currency="usd",
            quote_currency="cop",
            rate=Decimal("4100.55"),
            valid_from=now,
            valid_to=now - timedelta(days=1),
        )


def test_fx_currency_codes_are_normalised() -> None:
    rate = FxRate(
        source="banrep-trm",
        base_currency="usd",
        quote_currency="cop",
        rate=Decimal("4100.55"),
        valid_from=datetime(2026, 3, 14, tzinfo=UTC),
        valid_to=datetime(2026, 3, 15, tzinfo=UTC),
    )
    assert (rate.base_currency, rate.quote_currency) == ("USD", "COP")


# --------------------------------------------------------------------------
# Keys and lag
# --------------------------------------------------------------------------
def test_natural_keys_are_stable_and_include_the_venue(trade: Trade) -> None:
    """The venue belongs in the key: trade_id is only unique within an exchange."""
    assert trade.natural_key == ("binance", "BTCUSDT", 1_234_567)


def test_partition_key_keeps_a_symbol_on_one_partition(trade: Trade) -> None:
    assert trade.partition_key == trade.symbol


def test_lag_is_measured_from_the_venue_event_time(trade: Trade) -> None:
    assert trade.lag_millis() == 38


def test_dead_letter_error_type_is_low_cardinality() -> None:
    """error_type must be safe as a Prometheus label; the message must not be."""
    envelope = DeadLetter.from_exception(
        ValueError("symbol 'BTC/USD' is not a known instrument"),
        origin_topic="md.trades.v1",
        raw_payload='{"s":"BTC/USD"}',
        origin_stream="btcusd@trade",
    )
    assert envelope.error_type == "ValueError"
    assert "BTC/USD" in envelope.error_message
    assert envelope.raw_payload == '{"s":"BTC/USD"}'


def test_dead_letter_truncates_hostile_payloads() -> None:
    envelope = DeadLetter.from_exception(
        RuntimeError("x" * 10_000),
        origin_topic="md.trades.v1",
        raw_payload="y" * 200_000,
    )
    assert len(envelope.error_message) <= 4_000
    assert len(envelope.raw_payload) <= 64_000
