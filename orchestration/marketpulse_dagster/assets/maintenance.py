"""Iceberg table maintenance.

ADR-0002 accepted metadata growth as the cost of atomic commits and time
travel. This module is where that cost is paid, on a schedule, rather than
discovered six weeks later as a query planner that has become mysteriously slow.

The three operations are genuinely different and must run in this order:

1. **Compaction** rewrites many small files into few large ones. Streaming
   writes produce a file per partition per micro-batch -- 1,440 commits a day
   per table -- and query planning cost is dominated by file count, not row
   count.
2. **Snapshot expiry** drops old snapshots and the data files only they
   referenced. Without it, compaction *increases* storage forever, because the
   pre-compaction files stay reachable from older snapshots.
3. **Orphan removal** deletes files that no snapshot references at all,
   which is what a failed or aborted write leaves behind.

Running expiry before compaction is the classic mistake: it does nothing,
because the files compaction is about to orphan are still live.
"""

from __future__ import annotations

from datetime import timedelta

from dagster import (
    AssetExecutionContext,
    MetadataValue,
    Output,
    asset,
)

from marketpulse_dagster.resources import TrinoResource

__all__ = ["compact_iceberg_tables", "expire_iceberg_snapshots", "remove_orphan_files"]

#: Tables large or hot enough to be worth maintaining. The gold marts are
#: rebuilt in full each run and never accumulate small files.
MAINTAINED_TABLES = [
    "bronze.trades",
    "bronze.book_ticker",
    "bronze.klines",
    "silver.slv_trades",
    "silver.slv_quotes",
    "silver.slv_ohlcv_1m",
    "silver.slv_quote_metrics_1m",
]

SNAPSHOT_RETENTION_DAYS = 7


@asset(
    group_name="maintenance",
    compute_kind="trino",
    description=(
        "Rewrites small files into ~128MB targets. Query planning cost is "
        "dominated by file count, and a 60-second streaming trigger produces "
        "1,440 commits a day per table."
    ),
)
def compact_iceberg_tables(
    context: AssetExecutionContext, trino: TrinoResource
) -> Output[None]:
    results: dict[str, object] = {}
    for table in MAINTAINED_TABLES:
        # Iceberg exposes its own metadata as queryable tables. Counting files
        # before and after turns "maintenance ran" into "maintenance did
        # something", which is the difference between a green run and a useful
        # one -- a compaction that silently no-ops is the failure mode here.
        before = _file_count(trino, table)
        try:
            trino.query(
                f"ALTER TABLE lakehouse.{table} "
                "EXECUTE optimize(file_size_threshold => '96MB')"
            )
            after = _file_count(trino, table)
            results[table] = {"files_before": before, "files_after": after}
            context.log.info("compacted %s: %s -> %s files", table, before, after)
        except Exception as exc:  # noqa: BLE001 - one table must not stop the rest
            results[table] = {"error": str(exc)[:500]}
            context.log.error("compaction failed for %s: %s", table, exc)

    failures = [t for t, status in results.items() if isinstance(status, dict) and "error" in status]
    reclaimed = sum(
        status["files_before"] - status["files_after"]  # type: ignore[operator, index]
        for status in results.values()
        if isinstance(status, dict) and "files_after" in status
    )
    return Output(
        None,
        metadata={
            "tables": MetadataValue.json(results),
            "failed_tables": MetadataValue.int(len(failures)),
            "files_eliminated": MetadataValue.int(int(reclaimed)),
        },
    )


def _file_count(trino: TrinoResource, table: str) -> int:
    """Data files currently referenced by the table's live snapshot.

    Read from Iceberg's `$files` metadata table rather than by listing object
    storage, which would be both slow and wrong -- listing sees orphans and
    files held only by expired snapshots.
    """
    namespace, name = table.split(".", 1)
    try:
        return int(trino.scalar(f'select count(*) from lakehouse.{namespace}."{name}$files"', 0))
    except Exception:  # noqa: BLE001 - metadata table may not exist before first write
        return 0


@asset(
    group_name="maintenance",
    compute_kind="trino",
    # Ordering matters: expiry before compaction is a no-op, because the files
    # compaction is about to orphan are still referenced by the live snapshot.
    deps=[compact_iceberg_tables],
    description=(
        f"Expires snapshots older than {SNAPSHOT_RETENTION_DAYS} days. Without "
        "this, compaction increases storage forever, since pre-compaction files "
        "stay reachable from older snapshots."
    ),
)
def expire_iceberg_snapshots(
    context: AssetExecutionContext, trino: TrinoResource
) -> Output[None]:
    retention = f"{SNAPSHOT_RETENTION_DAYS}d"
    results: dict[str, str] = {}
    for table in MAINTAINED_TABLES:
        try:
            trino.query(
                f"ALTER TABLE lakehouse.{table} EXECUTE expire_snapshots(retention_threshold => '{retention}')"
            )
            results[table] = "expired"
        except Exception as exc:  # noqa: BLE001
            results[table] = f"failed: {exc}"
            context.log.error("snapshot expiry failed for %s: %s", table, exc)

    return Output(
        None,
        metadata={
            "retention": MetadataValue.text(retention),
            "tables": MetadataValue.json(results),
            # Time travel is bounded by this. Anyone relying on `FOR VERSION AS
            # OF` beyond the window needs to know where the limit comes from.
            "time_travel_horizon": MetadataValue.text(
                str(timedelta(days=SNAPSHOT_RETENTION_DAYS))
            ),
        },
    )


@asset(
    group_name="maintenance",
    compute_kind="trino",
    deps=[expire_iceberg_snapshots],
    description=(
        "Removes files no snapshot references -- what a failed or aborted write "
        "leaves behind. Runs last, and with a conservative threshold: an "
        "aggressive one can delete files an in-flight write has created but not "
        "yet committed."
    ),
)
def remove_orphan_files(context: AssetExecutionContext, trino: TrinoResource) -> Output[None]:
    results: dict[str, str] = {}
    for table in MAINTAINED_TABLES:
        try:
            # 7 days, not hours. A streaming write in progress has files on
            # object storage that no snapshot references yet; deleting those
            # corrupts the commit that was about to happen.
            trino.query(
                f"ALTER TABLE lakehouse.{table} EXECUTE remove_orphan_files(retention_threshold => '7d')"
            )
            results[table] = "cleaned"
        except Exception as exc:  # noqa: BLE001
            results[table] = f"failed: {exc}"
            context.log.error("orphan removal failed for %s: %s", table, exc)

    return Output(None, metadata={"tables": MetadataValue.json(results)})
