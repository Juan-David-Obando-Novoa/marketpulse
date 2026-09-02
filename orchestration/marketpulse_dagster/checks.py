"""Asset checks: data quality attached to the asset it guards.

The reason these are asset checks rather than a separate test job is where they
appear. A consumer looking at ``fct_market_1m`` in the UI and asking "is this
table good right now" gets the answer on the same object, instead of having to
know that a job called ``dq_suite`` exists and to go read its last run.

Division of labour with dbt: dbt tests assert things that are true of the data
*by construction* -- uniqueness, referential integrity, value ranges. The checks
here assert things that are only true if the *platform is working*: freshness,
volume against a trailing baseline, agreement with the venue. Those need a
clock and a history, which is not what a dbt test is for.
"""

from __future__ import annotations

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetKey,
    MetadataValue,
    asset_check,
)

from marketpulse_dagster.resources import TrinoResource

__all__ = [
    "check_no_decode_quarantine",
    "check_reconciliation_against_venue",
    "check_silver_freshness",
    "check_trade_volume_within_baseline",
    "check_all_tracked_symbols_present",
]


@asset_check(
    asset=AssetKey(["silver", "slv_trades"]),
    description="Silver trades must be no more than fifteen minutes behind the venue clock.",
    blocking=True,
)
def check_silver_freshness(trino: TrinoResource) -> AssetCheckResult:
    """Freshness measured in venue event time, not wall-clock of the last run.

    A model that runs successfully every five minutes and writes nothing is
    'fresh' by run time and stale by any definition that matters.
    """
    lag_seconds = trino.scalar(
        "select date_diff('second', max(event_time), current_timestamp) "
        "from lakehouse.silver.slv_trades"
    )
    if lag_seconds is None:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description="slv_trades is empty",
        )

    lag_seconds = int(lag_seconds)
    return AssetCheckResult(
        passed=lag_seconds <= 900,
        severity=AssetCheckSeverity.ERROR if lag_seconds > 3_600 else AssetCheckSeverity.WARN,
        metadata={
            "lag_seconds": MetadataValue.int(lag_seconds),
            "threshold_seconds": MetadataValue.int(900),
        },
    )


@asset_check(
    asset=AssetKey(["silver", "slv_trades"]),
    description=(
        "Today's trade volume must be within an order of magnitude of the "
        "trailing 7-day median for the same hour of day."
    ),
)
def check_trade_volume_within_baseline(trino: TrinoResource) -> AssetCheckResult:
    """Volume anomaly detection against the table's own history.

    Compared against the same hour of day rather than a flat average: market
    activity has a strong intraday shape, and a flat baseline would fire every
    night and never during a genuine daytime outage -- exactly backwards.

    Bounds are deliberately wide. This is a check for 'the pipeline stopped' or
    'the pipeline double-wrote', not for market conditions; a tight band on
    crypto volume would page on every news event.
    """
    rows = trino.query(
        """
        with hourly as (
            select
                date_trunc('hour', event_time) as hour_start,
                count(*)                       as trades
            from lakehouse.silver.slv_trades
            where event_time >= current_timestamp - interval '8' day
            group by 1
        ),
        latest as (
            select trades, hour_start from hourly order by hour_start desc offset 1 limit 1
        ),
        baseline as (
            select approx_percentile(trades, 0.5) as median_trades
            from hourly
            where hour_start < (select hour_start from latest)
              and hour(hour_start) = (select hour(hour_start) from latest)
        )
        select
            (select trades from latest),
            (select median_trades from baseline),
            (select hour_start from latest)
        """
    )
    if not rows or rows[0][0] is None or rows[0][1] is None:
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.WARN,
            description="not enough history for a baseline yet",
        )

    actual, baseline, hour_start = int(rows[0][0]), float(rows[0][1]), rows[0][2]
    ratio = actual / baseline if baseline else 0.0
    return AssetCheckResult(
        passed=0.1 <= ratio <= 10.0,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "hour": MetadataValue.text(str(hour_start)),
            "trades": MetadataValue.int(actual),
            "baseline_median": MetadataValue.float(round(baseline, 1)),
            "ratio": MetadataValue.float(round(ratio, 3)),
        },
    )


@asset_check(
    asset=AssetKey(["gold", "fct_kline_reconciliation"]),
    description="At least 98% of reconciled minutes must agree with the venue's own candles.",
    blocking=True,
)
def check_reconciliation_against_venue(trino: TrinoResource) -> AssetCheckResult:
    """The one check that can catch a dropped or double-counted trade.

    Every other assertion in the platform compares our data to our own
    expectations, and a missing trade satisfies all of them.
    """
    rows = trino.query(
        """
        select
            count(*)                                                    as total,
            sum(case when reconciliation_status = 'ok' then 1 else 0 end) as matched,
            max(volume_rel_diff)                                        as worst_volume_diff
        from lakehouse.gold.fct_kline_reconciliation
        where bar_start >= current_timestamp - interval '24' hour
        """
    )
    total, matched, worst = rows[0] if rows else (0, 0, None)
    if not total:
        return AssetCheckResult(
            passed=True,
            severity=AssetCheckSeverity.WARN,
            description="no overlapping window to reconcile in the last 24h",
        )

    pass_ratio = (matched or 0) / total
    return AssetCheckResult(
        passed=pass_ratio >= 0.98,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "minutes_compared": MetadataValue.int(int(total)),
            "minutes_matched": MetadataValue.int(int(matched or 0)),
            "pass_ratio": MetadataValue.float(round(pass_ratio, 4)),
            "worst_volume_rel_diff": MetadataValue.float(float(worst or 0)),
        },
    )


@asset_check(
    asset=AssetKey("bronze_reference_load"),
    description="No message may fail Avro decoding: that means a producer broke the contract.",
    blocking=True,
)
def check_no_decode_quarantine(trino: TrinoResource) -> AssetCheckResult:
    """A non-empty quarantine is a contract violation, not a data-quality blip.

    ADR-0006 promises BACKWARD compatibility. If a message cannot be decoded by
    the current reader schema, that promise was broken somewhere, and no amount
    of downstream cleaning fixes it.
    """
    count = int(
        trino.scalar(
            "select count(*) from lakehouse.ops.decode_quarantine "
            "where _ingested_at >= current_timestamp - interval '24' hour",
            0,
        )
    )
    return AssetCheckResult(
        passed=count == 0,
        severity=AssetCheckSeverity.ERROR,
        metadata={"quarantined_last_24h": MetadataValue.int(count)},
        description=(
            "see ADR-0006; inspect lakehouse.ops.decode_quarantine" if count else "clean"
        ),
    )


@asset_check(
    asset=AssetKey(["gold", "dim_instrument"]),
    description="Every instrument the platform claims to track must actually be arriving.",
)
def check_all_tracked_symbols_present(trino: TrinoResource) -> AssetCheckResult:
    """Absence of data is invisible unless something asserts on the expected set.

    No query over the lake can notice a symbol that never arrived; only a
    comparison against the declared reference can.
    """
    rows = trino.query(
        "select symbol, coverage_status from lakehouse.gold.dim_instrument "
        "where coverage_status <> 'ok'"
    )
    problems = {symbol: status for symbol, status in rows}
    missing = [s for s, status in problems.items() if status == "tracked_but_absent"]
    return AssetCheckResult(
        passed=not missing,
        severity=AssetCheckSeverity.ERROR if missing else AssetCheckSeverity.WARN,
        metadata={
            "tracked_but_absent": MetadataValue.json(missing),
            "all_anomalies": MetadataValue.json(problems),
        },
    )
