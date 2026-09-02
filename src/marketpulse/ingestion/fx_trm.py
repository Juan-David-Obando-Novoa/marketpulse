"""Reference FX ingestion: the Colombian TRM.

Every price in this platform is quoted in USD. A Colombian consumer needs COP,
and the only defensible conversion is the official rate -- the TRM published by
the Banco de la Republica and redistributed through the national open-data
portal as a Socrata dataset.

The interesting modelling problem is that the TRM is **an interval, not a
point**. The rate published on a Friday is legally in force through the
weekend, and around holidays a single publication can cover four days. Storing
it as "the rate on date D" forces every consumer to reimplement that
gap-filling, and they will each get the boundary slightly wrong. Storing
``[valid_from, valid_to)`` makes the conversion a range join and the ambiguity
disappears.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

from marketpulse.contracts.models import FxRate
from marketpulse.ingestion.normalizers import NormalizationError
from marketpulse.logging import get_logger
from marketpulse.utils.timeutil import utc_now

if TYPE_CHECKING:
    from marketpulse.config import FxSettings

__all__ = ["SOURCE", "TrmClient", "normalise_trm_row"]

log = get_logger(__name__)

SOURCE = "banrep-trm"
BASE_CURRENCY = "USD"
QUOTE_CURRENCY = "COP"


def _parse_socrata_timestamp(raw: str) -> datetime:
    """Parse Socrata's floating timestamps as Bogota local time.

    Socrata serialises these without an offset. They are Colombian civil dates,
    so interpreting them as UTC would shift every boundary by five hours and
    silently attribute Monday's rate to part of Sunday.
    """
    naive = datetime.fromisoformat(raw.replace("Z", ""))
    bogota = timedelta(hours=-5)  # Colombia has had no DST since 1993.
    from datetime import timezone  # noqa: PLC0415

    return naive.replace(tzinfo=timezone(bogota))


def normalise_trm_row(row: dict[str, Any], *, producer_id: str) -> FxRate:
    """Map one Socrata row onto :class:`FxRate`.

    Source fields: ``valor`` (the rate), ``unidad`` (always COP),
    ``vigenciadesde`` and ``vigenciahasta`` (the validity interval, inclusive
    on both ends in the source). ``valid_to`` is converted to an exclusive
    bound by adding a day, so intervals tile without overlap.
    """
    try:
        rate = Decimal(str(row["valor"]))
    except (KeyError, InvalidOperation, TypeError) as exc:
        raise NormalizationError(f"TRM row has no usable 'valor': {row!r}") from exc

    try:
        valid_from = _parse_socrata_timestamp(str(row["vigenciadesde"]))
        valid_to_inclusive = _parse_socrata_timestamp(str(row["vigenciahasta"]))
    except (KeyError, ValueError) as exc:
        raise NormalizationError(f"TRM row has an unparseable validity interval: {exc}") from exc

    quote = str(row.get("unidad", QUOTE_CURRENCY)).strip().upper() or QUOTE_CURRENCY

    import json  # noqa: PLC0415

    return FxRate(
        source=SOURCE,
        base_currency=BASE_CURRENCY,
        quote_currency=quote,
        rate=rate,
        valid_from=valid_from,
        valid_to=valid_to_inclusive + timedelta(days=1),
        ingested_at=utc_now(),
        producer_id=producer_id,
        raw_payload=json.dumps(row, separators=(",", ":")),
    )


class TrmClient:
    """Socrata client for the TRM dataset.

    Small on purpose: one dataset, one query shape, paginated. The Socrata SoQL
    parameters are passed explicitly rather than through a query builder,
    because the query is fixed and a builder would only hide it.
    """

    def __init__(self, settings: FxSettings, session: Any, *, producer_id: str = "fx") -> None:
        self._settings = settings
        self._session = session
        self._producer_id = producer_id

    @classmethod
    async def create(cls, settings: FxSettings, *, producer_id: str = "fx") -> TrmClient:
        import aiohttp  # noqa: PLC0415

        headers = {"User-Agent": "marketpulse/0.1"}
        if settings.app_token is not None:
            # Socrata throttles anonymous callers hard; the token raises the
            # limit and is not a credential in the secret sense, but it is
            # still handled as one.
            headers["X-App-Token"] = settings.app_token.get_secret_value()
        session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60), headers=headers)
        return cls(settings, session, producer_id=producer_id)

    async def close(self) -> None:
        close = getattr(self._session, "close", None)
        if close is not None:
            await close()

    async def fetch_since(self, since: datetime, *, limit: int | None = None) -> list[FxRate]:
        """Fetch every published rate whose validity starts at or after ``since``.

        Rows that fail normalisation are logged and skipped rather than
        aborting the batch: one malformed historical row should not prevent
        today's rate from landing.
        """
        url = f"{self._settings.socrata_base_url}/{self._settings.trm_dataset_id}.json"
        params = {
            "$where": f"vigenciadesde >= '{since.date().isoformat()}T00:00:00.000'",
            "$order": "vigenciadesde ASC",
            "$limit": str(limit or self._settings.page_size),
        }
        async with self._session.get(url, params=params) as response:
            if response.status != 200:
                body = await response.text()
                raise RuntimeError(f"TRM fetch failed with {response.status}: {body[:300]}")
            rows = await response.json()

        rates: list[FxRate] = []
        skipped = 0
        for row in rows:
            try:
                rates.append(normalise_trm_row(row, producer_id=self._producer_id))
            except (NormalizationError, ValueError) as exc:
                skipped += 1
                log.warning("trm.row_skipped", error=str(exc))
        log.info("trm.fetched", rows=len(rows), accepted=len(rates), skipped=skipped)
        return rates
