"""Partition definitions.

The partition scheme is the thing that makes a backfill a UI selection rather
than a hand-written script, so it is worth getting the granularity right.

Daily, in UTC, because that is the grain of both the Iceberg partition spec and
the venue's own reporting. Hourly would multiply the run count by 24 for no
gain: nothing in this platform is reprocessed at hour granularity.
"""

from __future__ import annotations

from dagster import (
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)

__all__ = [
    "DAILY_PARTITIONS",
    "PLATFORM_EPOCH",
    "SYMBOL_DAILY_PARTITIONS",
    "SYMBOL_PARTITIONS",
]

#: The earliest date the platform claims to have data for. Backfills before
#: this are refused rather than silently producing empty partitions.
PLATFORM_EPOCH = "2026-01-01"

DAILY_PARTITIONS = DailyPartitionsDefinition(
    start_date=PLATFORM_EPOCH,
    timezone="UTC",
    # One hour of slack before a day is considered complete: a late REST
    # backfill or a slow micro-batch must not race the partition it belongs to.
    end_offset=0,
)

SYMBOL_PARTITIONS = StaticPartitionsDefinition(
    ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT"]
)

#: Two-dimensional partitioning for the backfill asset only. Backfilling one
#: symbol over a date range is the common operation -- a new instrument is
#: added far more often than the whole history is rebuilt -- and a single
#: date-only partition would force re-fetching every symbol to fix one.
SYMBOL_DAILY_PARTITIONS = MultiPartitionsDefinition(
    {"date": DAILY_PARTITIONS, "symbol": SYMBOL_PARTITIONS}
)
