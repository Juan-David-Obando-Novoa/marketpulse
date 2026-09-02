"""Iceberg table maintenance, runnable standalone.

The Dagster assets in ``orchestration/`` drive this on a schedule through
Trino's ``ALTER TABLE ... EXECUTE`` procedures. This module is the other path:
a PyIceberg implementation that needs neither a Trino coordinator nor a Spark
cluster, for the case where the platform is degraded and maintenance is
precisely what is needed to recover it.

    python -m marketpulse.maintenance.iceberg_maintenance --all
    python -m marketpulse.maintenance.iceberg_maintenance --table bronze.trades --dry-run

Why maintenance is a first-class workload rather than a cron afterthought:
ADR-0002 accepted metadata growth as the price of atomic commits and time
travel. A 60-second streaming trigger produces 1,440 commits per table per day.
Query planning cost is dominated by file count, so an unmaintained table does
not fail -- it gets slower, gradually, until someone concludes the lakehouse
was a mistake.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from marketpulse.config import get_settings
from marketpulse.logging import configure_logging, get_logger

log = get_logger(__name__)
UTC = timezone.utc

__all__ = ["MaintenanceReport", "TableMaintenance", "main"]

#: Tables worth maintaining: the streaming targets and the incremental silver
#: models. Gold marts are rebuilt in full and never accumulate small files.
DEFAULT_TABLES = (
    "bronze.trades",
    "bronze.book_ticker",
    "bronze.klines",
    "silver.slv_trades",
    "silver.slv_quotes",
    "silver.slv_ohlcv_1m",
    "silver.slv_quote_metrics_1m",
)


@dataclass
class MaintenanceReport:
    """What maintenance actually did, per table.

    Reported rather than logged-and-forgotten because "maintenance ran" and
    "maintenance did something" are different claims, and a compaction that
    silently no-ops is the failure mode here.
    """

    table: str
    snapshots_before: int = 0
    snapshots_after: int = 0
    data_files_before: int = 0
    data_files_after: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def snapshots_expired(self) -> int:
        return max(self.snapshots_before - self.snapshots_after, 0)

    @property
    def files_eliminated(self) -> int:
        return max(self.data_files_before - self.data_files_after, 0)

    @property
    def ok(self) -> bool:
        return not self.errors

    def as_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "snapshots_before": self.snapshots_before,
            "snapshots_after": self.snapshots_after,
            "snapshots_expired": self.snapshots_expired,
            "data_files_before": self.data_files_before,
            "data_files_after": self.data_files_after,
            "files_eliminated": self.files_eliminated,
            "errors": self.errors,
        }


class TableMaintenance:
    """Maintenance operations against one Iceberg catalog."""

    def __init__(self, catalog: Any, *, retention_days: int = 7, dry_run: bool = False) -> None:
        self._catalog = catalog
        self._retention = timedelta(days=retention_days)
        self._dry_run = dry_run

    @classmethod
    def from_settings(cls, *, dry_run: bool = False) -> TableMaintenance:
        from pyiceberg.catalog import load_catalog  # noqa: PLC0415

        settings = get_settings()
        catalog = load_catalog(
            settings.iceberg.catalog_name,
            **{
                "uri": settings.iceberg.rest_uri,
                "warehouse": settings.s3.warehouse_uri,
                "s3.endpoint": settings.s3.endpoint,
                "s3.access-key-id": settings.s3.access_key.get_secret_value(),
                "s3.secret-access-key": settings.s3.secret_key.get_secret_value(),
                "s3.path-style-access": "true",
            },
        )
        return cls(
            catalog,
            retention_days=settings.iceberg.snapshot_retention_days,
            dry_run=dry_run,
        )

    # ------------------------------------------------------------------
    def inspect(self, identifier: str) -> tuple[int, int]:
        """Return (snapshot count, data-file count) for a table.

        File count comes from the manifests, not from listing object storage:
        listing sees orphans and files held only by expired snapshots, so it
        answers a different and less useful question.
        """
        table = self._catalog.load_table(identifier)
        snapshots = len(list(table.metadata.snapshots))
        current = table.current_snapshot()
        if current is None:
            return snapshots, 0
        data_files = sum(
            manifest.added_files_count or 0 + (manifest.existing_files_count or 0)
            for manifest in current.manifests(table.io)
        )
        return snapshots, int(data_files)

    def expire_snapshots(self, identifier: str) -> None:
        """Drop snapshots older than the retention window.

        This is what bounds time travel: after it runs, ``FOR VERSION AS OF``
        beyond the window fails. That is a deliberate trade -- unbounded time
        travel means unbounded storage, and the retention is configuration
        rather than an accident of never having run this.
        """
        cutoff = datetime.now(tz=UTC) - self._retention
        table = self._catalog.load_table(identifier)
        if self._dry_run:
            expiring = [
                snapshot.snapshot_id
                for snapshot in table.metadata.snapshots
                if datetime.fromtimestamp(snapshot.timestamp_ms / 1000, tz=UTC) < cutoff
            ]
            log.info("dry_run.expire", table=identifier, would_expire=len(expiring))
            return
        table.expire_snapshots().expire_snapshots_older_than(cutoff).commit()

    def rewrite_manifests(self, identifier: str) -> None:
        """Consolidate manifest files.

        Separate from data-file compaction and much cheaper. Streaming writes
        add a manifest per commit; thousands of tiny manifests slow planning
        even when the data files themselves are healthy.
        """
        if self._dry_run:
            log.info("dry_run.rewrite_manifests", table=identifier)
            return
        table = self._catalog.load_table(identifier)
        table.rewrite_manifests()

    def run(self, identifier: str) -> MaintenanceReport:
        """Full maintenance pass over one table, in the only correct order."""
        report = MaintenanceReport(table=identifier)
        try:
            report.snapshots_before, report.data_files_before = self.inspect(identifier)
        except Exception as exc:  # noqa: BLE001
            report.errors.append(f"inspect failed: {exc}")
            return report

        # Manifests first, then expiry. Reversing these wastes the expiry pass:
        # the manifests it would have dropped are still referenced.
        for step, operation in (
            ("rewrite_manifests", self.rewrite_manifests),
            ("expire_snapshots", self.expire_snapshots),
        ):
            try:
                operation(identifier)
            except Exception as exc:  # noqa: BLE001 - one step must not stop the rest
                report.errors.append(f"{step} failed: {exc}")
                log.error("maintenance.step_failed", table=identifier, step=step, error=str(exc))

        try:
            report.snapshots_after, report.data_files_after = self.inspect(identifier)
        except Exception as exc:  # noqa: BLE001
            report.errors.append(f"post-inspect failed: {exc}")
        return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="marketpulse-maintenance",
        description="Compact manifests and expire snapshots on Iceberg tables.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true", help="Maintain every default table.")
    group.add_argument("--table", action="append", help="Table to maintain, e.g. bronze.trades.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be expired without committing anything.",
    )
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(
        level=settings.observability.log_level,
        json_logs=settings.observability.json_logs,
        service_name="marketpulse-maintenance",
    )

    tables = list(DEFAULT_TABLES) if args.all else list(args.table or ())
    maintenance = TableMaintenance.from_settings(dry_run=args.dry_run)

    reports = [maintenance.run(table) for table in tables]
    for report in reports:
        log.info("maintenance.table", **report.as_dict())

    failed = [report for report in reports if not report.ok]
    log.info(
        "maintenance.complete",
        tables=len(reports),
        failed=len(failed),
        snapshots_expired=sum(r.snapshots_expired for r in reports),
        files_eliminated=sum(r.files_eliminated for r in reports),
        dry_run=args.dry_run,
    )
    # Non-zero on any failure so a scheduled run surfaces rather than going
    # green with errors buried in the log.
    return 1 if failed else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
