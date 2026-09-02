"""Contract compatibility against a live registry.

This is the ADR-0006 gate. It is an integration test rather than a unit test
because compatibility is a property of the *registry's* history, not of our
files -- the question "would this break an existing reader" cannot be answered
without knowing what readers were promised.
"""

from __future__ import annotations

import pytest

from marketpulse.config import AppSettings
from marketpulse.contracts.models import BookTicker, DeadLetter, FxRate, Kline, Trade
from marketpulse.contracts.registry import (
    SchemaRegistryClient,
    load_schema,
    subject_for,
)

pytestmark = pytest.mark.integration

STREAM_MODELS = {
    "trades": Trade,
    "book_ticker": BookTicker,
    "klines": Kline,
    "fx_rates": FxRate,
    "dead_letter": DeadLetter,
}


@pytest.fixture
def registry(schema_registry_url: str) -> SchemaRegistryClient:
    return SchemaRegistryClient(schema_registry_url)


@pytest.mark.parametrize("stream", sorted(STREAM_MODELS))
def test_local_schema_is_backward_compatible(
    registry: SchemaRegistryClient, stream: str
) -> None:
    """A schema that fails here would break a running consumer on deploy."""
    settings = AppSettings()
    subject = subject_for(settings.kafka.topics[stream])
    schema = load_schema(STREAM_MODELS[stream].avro_schema_file)
    assert registry.check_compatibility(subject, schema), (
        f"{subject} is not BACKWARD compatible; see "
        "docs/adr/0006-schema-evolution-and-data-contracts.md"
    )


def test_registration_is_idempotent(registry: SchemaRegistryClient) -> None:
    """Registering an unchanged schema must return the same id, not a new version."""
    settings = AppSettings()
    subject = subject_for(settings.kafka.topics["trades"])
    schema = load_schema(Trade.avro_schema_file)
    assert registry.register(subject, schema) == registry.register(subject, schema)


def test_removing_a_required_field_is_rejected(registry: SchemaRegistryClient) -> None:
    """The guarantee, asserted directly rather than assumed.

    Dropping `price` from the trade contract must be refused by the registry.
    If this ever passes, BACKWARD is not actually enforced and every claim
    ADR-0006 makes is void.
    """
    settings = AppSettings()
    subject = subject_for(settings.kafka.topics["trades"])
    schema = load_schema(Trade.avro_schema_file)
    registry.register(subject, schema)

    mutilated = {
        **schema,
        "fields": [f for f in schema["fields"] if f["name"] != "price"],
    }
    assert not registry.check_compatibility(subject, mutilated)
