"""The TRM feed: interval semantics and the Bogota timezone trap."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from marketpulse.config import FxSettings
from marketpulse.ingestion.fx_trm import SOURCE, TrmClient, normalise_trm_row
from marketpulse.ingestion.normalizers import NormalizationError
from tests.unit.fakes import FakeResponse, FakeSession

pytestmark = pytest.mark.unit

# A Friday publication, valid through the weekend -- the case that makes this
# a bitemporal interval rather than a daily point.
TRM_ROW = {
    "valor": "4123.45",
    "unidad": "COP",
    "vigenciadesde": "2026-03-13T00:00:00.000",
    "vigenciahasta": "2026-03-15T00:00:00.000",
}


def test_row_maps_onto_the_contract() -> None:
    rate = normalise_trm_row(TRM_ROW, producer_id="fx")
    assert rate.source == SOURCE
    assert (rate.base_currency, rate.quote_currency) == ("USD", "COP")
    assert rate.rate == Decimal("4123.45")


def test_timestamps_are_bogota_not_utc() -> None:
    """Socrata sends no offset. Reading these as UTC shifts every boundary by five hours."""
    rate = normalise_trm_row(TRM_ROW, producer_id="fx")
    assert rate.valid_from.utcoffset() == timedelta(hours=-5)
    assert rate.valid_from.hour == 0


def test_inclusive_source_bound_becomes_an_exclusive_contract_bound() -> None:
    """Source intervals are closed-closed; ours are closed-open so they tile."""
    rate = normalise_trm_row(TRM_ROW, producer_id="fx")
    assert (rate.valid_to - rate.valid_from) == timedelta(days=3)


def test_weekend_rate_covers_saturday_and_sunday() -> None:
    rate = normalise_trm_row(TRM_ROW, producer_id="fx")
    span_days = (rate.valid_to - rate.valid_from).days
    assert span_days == 3, "Friday publication must remain in force through Sunday"


def test_raw_payload_is_retained() -> None:
    assert normalise_trm_row(TRM_ROW, producer_id="fx").raw_payload is not None


@pytest.mark.parametrize("field", ["valor", "vigenciadesde", "vigenciahasta"])
def test_missing_field_is_a_normalization_error(field: str) -> None:
    row = {k: v for k, v in TRM_ROW.items() if k != field}
    with pytest.raises(NormalizationError):
        normalise_trm_row(row, producer_id="fx")


def test_non_numeric_rate_is_rejected() -> None:
    with pytest.raises(NormalizationError, match="valor"):
        normalise_trm_row({**TRM_ROW, "valor": "n/d"}, producer_id="fx")


async def test_fetch_since_builds_a_soql_filter() -> None:
    session = FakeSession([FakeResponse(payload=[TRM_ROW])])
    client = TrmClient(FxSettings(), session)

    rates = await client.fetch_since(datetime(2026, 1, 1, tzinfo=timezone.utc))

    assert len(rates) == 1
    _, params = session.requests[0]
    assert params["$where"].startswith("vigenciadesde >= '2026-01-01")
    assert params["$order"] == "vigenciadesde ASC"


async def test_one_bad_row_does_not_discard_the_batch() -> None:
    """Today's rate must still land even if a historical row is malformed."""
    session = FakeSession([FakeResponse(payload=[{"valor": "oops"}, TRM_ROW])])
    rates = await TrmClient(FxSettings(), session).fetch_since(
        datetime(2026, 1, 1, tzinfo=timezone.utc)
    )
    assert len(rates) == 1


async def test_http_error_is_surfaced() -> None:
    session = FakeSession([FakeResponse(status=500, text="portal down")])
    with pytest.raises(RuntimeError, match="500"):
        await TrmClient(FxSettings(), session).fetch_since(
            datetime(2026, 1, 1, tzinfo=timezone.utc)
        )
