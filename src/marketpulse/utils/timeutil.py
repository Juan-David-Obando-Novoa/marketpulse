"""Time handling.

Exchange APIs speak epoch milliseconds; Iceberg and dbt speak timestamps with
timezone. Every conversion in this codebase goes through these helpers so that
a naive datetime can never reach a table.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

UTC = timezone.utc

__all__ = [
    "datetime_to_epoch_millis",
    "day_range",
    "epoch_millis_to_datetime",
    "floor_to_interval",
    "utc_now",
]


def utc_now() -> datetime:
    """Timezone-aware ``now`` in UTC. Never use ``datetime.utcnow``."""
    return datetime.now(tz=UTC)


def epoch_millis_to_datetime(millis: int) -> datetime:
    """Convert exchange epoch milliseconds to an aware UTC datetime."""
    if millis < 0:
        raise ValueError(f"epoch milliseconds must be non-negative, got {millis}")
    return datetime.fromtimestamp(millis / 1_000, tz=UTC)


def datetime_to_epoch_millis(moment: datetime) -> int:
    """Convert an aware datetime to epoch milliseconds.

    Naive datetimes are rejected rather than silently assumed to be UTC: a
    silent assumption here is how an eight-hour offset reaches production.
    """
    if moment.tzinfo is None:
        raise ValueError("naive datetime rejected; attach a timezone explicitly")
    return int(moment.astimezone(UTC).timestamp() * 1_000)


def floor_to_interval(moment: datetime, interval: timedelta) -> datetime:
    """Floor ``moment`` to the start of its ``interval`` bucket.

    Used to assign trades to OHLCV candles without relying on the engine's
    ``date_trunc`` semantics, which differ between Spark and Trino.
    """
    if interval <= timedelta(0):
        raise ValueError("interval must be positive")
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    elapsed = (moment.astimezone(UTC) - epoch) // interval
    return epoch + elapsed * interval


def day_range(start: date, end: date) -> list[date]:
    """Inclusive list of dates from ``start`` to ``end``, used by backfills."""
    if end < start:
        raise ValueError(f"end {end} precedes start {start}")
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]
