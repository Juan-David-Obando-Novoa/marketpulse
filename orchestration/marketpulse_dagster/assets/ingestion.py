"""Ingestion assets.

Each of these is a *materialisable* asset -- something that produces data on a
schedule or a backfill -- as opposed to the streaming queries, which are
long-running services and are modelled separately in ``bronze.py``.

Every asset here shells out to the same CLI an engineer would run by hand. That
is a deliberate constraint: a Dagster failure is then reproducible by copying
one line out of the run log, rather than by reconstructing what the
orchestrator was doing internally.
"""

from __future__ import annotations

from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    Backoff,
    Jitter,
    MetadataValue,
    Output,
    RetryPolicy,
    asset,
)

from marketpulse_dagster.partitions import DAILY_PARTITIONS, SYMBOL_DAILY_PARTITIONS
from marketpulse_dagster.resources import MarketPulseCliResource

__all__ = ["binance_klines_backfill", "fx_reference_rates", "instrument_metadata"]

# A public API behind a rate limiter deserves a patient, jittered retry rather
# than three fast attempts that all bounce off the same 429.
API_RETRY = RetryPolicy(max_retries=4, delay=30, backoff=Backoff.EXPONENTIAL, jitter=Jitter.PLUS_MINUS)


@asset(
    partitions_def=SYMBOL_DAILY_PARTITIONS,
    group_name="ingestion",
    compute_kind="python",
    retry_policy=API_RETRY,
    description=(
        "Venue-published 1m candles for one symbol on one day. Partitioned by "
        "symbol as well as date because adding an instrument is far more common "
        "than rebuilding all history, and a date-only partition would force "
        "re-fetching every symbol to fix one."
    ),
)
def binance_klines_backfill(
    context: AssetExecutionContext, marketpulse_cli: MarketPulseCliResource
) -> Output[None]:
    keys = context.partition_key.keys_by_dimension
    symbol, date = keys["symbol"], keys["date"]

    from datetime import date as date_type, timedelta  # noqa: PLC0415

    start = date_type.fromisoformat(date)
    end = start + timedelta(days=1)

    output = marketpulse_cli.run(
        "backfill",
        "--symbol", symbol,
        "--start", start.isoformat(),
        "--end", end.isoformat(),
        "--interval", "1m",
    )

    # 1440 minutes in a day. Anything materially short is a venue gap or a
    # truncated response, and surfacing the count on the materialisation means
    # it is visible in the asset history without opening the logs.
    published = _extract_count(output)
    return Output(
        None,
        metadata={
            "symbol": symbol,
            "date": date,
            "candles_published": MetadataValue.int(published),
            "expected_candles": MetadataValue.int(1_440),
            "completeness": MetadataValue.float(round(published / 1_440, 4)),
            "cli_output": MetadataValue.text(output.strip()[:2_000]),
        },
    )


@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="ingestion",
    compute_kind="python",
    retry_policy=API_RETRY,
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    description=(
        "Official USD/COP reference rates. Fetches a 30-day trailing window "
        "rather than only the partition's own day: the publisher backfills "
        "corrections, and a window that only ever looks at today would never "
        "see them."
    ),
)
def fx_reference_rates(
    context: AssetExecutionContext, marketpulse_cli: MarketPulseCliResource
) -> Output[None]:
    from datetime import date as date_type, timedelta  # noqa: PLC0415

    partition_date = date_type.fromisoformat(context.partition_key)
    since = partition_date - timedelta(days=30)

    output = marketpulse_cli.run("fx-sync", "--since", since.isoformat())
    return Output(
        None,
        metadata={
            "since": since.isoformat(),
            "rates_published": MetadataValue.int(_extract_count(output)),
            "cli_output": MetadataValue.text(output.strip()[:2_000]),
        },
    )


@asset(
    partitions_def=DAILY_PARTITIONS,
    group_name="ingestion",
    compute_kind="python",
    retry_policy=API_RETRY,
    description=(
        "Snapshot of the venue's instrument filters. Feeds the SCD2 snapshot, "
        "which is what makes the tick-size assertion meaningful on historical "
        "data -- a trade that looks invalid against today's grid is usually "
        "valid against the grid in force when it printed."
    ),
)
def instrument_metadata(
    context: AssetExecutionContext, marketpulse_cli: MarketPulseCliResource
) -> Output[None]:
    output = marketpulse_cli.run("reference", "sync-instruments")
    return Output(
        None,
        metadata={
            "instruments": MetadataValue.int(_extract_count(output)),
            "observed_on": context.partition_key,
        },
    )


def _extract_count(cli_output: str) -> int:
    """Pull the leading integer out of a CLI summary line.

    The CLI prints a human sentence; this lifts the number onto the asset
    metadata so it becomes a plottable series in the Dagster UI rather than
    something you have to read logs to find.
    """
    for token in cli_output.split():
        if token.isdigit():
            return int(token)
    return 0
