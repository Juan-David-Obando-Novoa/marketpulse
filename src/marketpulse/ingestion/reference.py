"""Instrument reference synchronisation.

Pulls the venue's ``exchangeInfo`` and publishes one observation per
instrument. The observations are append-only and the SCD2 dimension is built
from them by dbt, rather than maintaining current-state here.

That split matters: the venue changes tick size and lot size without
announcement, and a trade that looks invalid against today's filters is almost
always valid against the ones in force when it printed. Overwriting a
current-state row would destroy the only evidence of when the change happened.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from marketpulse.logging import get_logger
from marketpulse.utils.timeutil import utc_now

log = get_logger(__name__)

__all__ = ["InstrumentObservation", "extract_filters", "observations_from_exchange_info"]


@dataclass(frozen=True, slots=True)
class InstrumentObservation:
    """One observation of an instrument's filters at a point in time."""

    symbol: str
    base_asset: str
    quote_asset: str
    status: str
    observed_at: datetime
    tick_size: Decimal | None
    step_size: Decimal | None
    min_notional: Decimal | None

    def as_row(self) -> dict[str, Any]:
        """Row shape for the bronze.instrument_metadata table."""
        return asdict(self)


def extract_filters(symbol_payload: dict[str, Any]) -> dict[str, Decimal | None]:
    """Pull tick size, step size and minimum notional out of the filters array.

    The venue returns filters as a list of heterogeneous objects keyed by
    ``filterType`` rather than as named fields, so this is the translation. A
    missing filter yields ``None`` rather than a default: a fabricated tick size
    would make the downstream grid assertion pass for the wrong reason.
    """
    by_type = {
        entry.get("filterType"): entry
        for entry in symbol_payload.get("filters", [])
        if isinstance(entry, dict)
    }

    def decimal_or_none(filter_type: str, key: str) -> Decimal | None:
        entry = by_type.get(filter_type)
        if not entry or key not in entry:
            return None
        try:
            return Decimal(str(entry[key]))
        except (TypeError, ValueError):
            return None

    return {
        "tick_size": decimal_or_none("PRICE_FILTER", "tickSize"),
        "step_size": decimal_or_none("LOT_SIZE", "stepSize"),
        "min_notional": decimal_or_none("NOTIONAL", "minNotional")
        or decimal_or_none("MIN_NOTIONAL", "minNotional"),
    }


def observations_from_exchange_info(
    payload: dict[str, Any],
    *,
    symbols: list[str] | None = None,
    observed_at: datetime | None = None,
) -> list[InstrumentObservation]:
    """Turn an ``exchangeInfo`` response into observation rows.

    ``observed_at`` is when we asked, not when the venue changed anything --
    the venue publishes no modification timestamp, which is exactly why the
    dbt snapshot uses the check strategy rather than a timestamp strategy.
    """
    timestamp = observed_at or utc_now()
    wanted = {s.upper() for s in symbols} if symbols else None

    rows: list[InstrumentObservation] = []
    for entry in payload.get("symbols", []):
        symbol = str(entry.get("symbol", "")).upper()
        if not symbol or (wanted and symbol not in wanted):
            continue
        filters = extract_filters(entry)
        rows.append(
            InstrumentObservation(
                symbol=symbol,
                base_asset=str(entry.get("baseAsset", "")).upper(),
                quote_asset=str(entry.get("quoteAsset", "")).upper(),
                status=str(entry.get("status", "UNKNOWN")).upper(),
                observed_at=timestamp,
                tick_size=filters["tick_size"],
                step_size=filters["step_size"],
                min_notional=filters["min_notional"],
            )
        )

    log.info("reference.observations_built", instruments=len(rows))
    return rows
