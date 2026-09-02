"""Maintenance reporting.

The reporting is what makes maintenance auditable, so it is what gets tested:
a compaction that silently does nothing is the failure mode, and 'the job was
green' is not evidence against it.
"""

from __future__ import annotations

import pytest

from marketpulse.maintenance.iceberg_maintenance import (
    DEFAULT_TABLES,
    MaintenanceReport,
    TableMaintenance,
)

pytestmark = pytest.mark.unit


def test_report_computes_what_was_reclaimed() -> None:
    report = MaintenanceReport(
        table="bronze.trades",
        snapshots_before=200,
        snapshots_after=48,
        data_files_before=5_000,
        data_files_after=120,
    )
    assert report.snapshots_expired == 152
    assert report.files_eliminated == 4_880
    assert report.ok


def test_a_growing_table_never_reports_negative_reclamation() -> None:
    """Concurrent writes can grow a table mid-pass; that is not negative work."""
    report = MaintenanceReport(
        table="bronze.trades", data_files_before=100, data_files_after=140
    )
    assert report.files_eliminated == 0


def test_errors_make_the_report_not_ok() -> None:
    report = MaintenanceReport(table="bronze.trades", errors=["expire failed: boom"])
    assert not report.ok
    assert report.as_dict()["errors"] == ["expire failed: boom"]


def test_default_tables_cover_streaming_and_incremental_targets_only() -> None:
    """Gold marts are rebuilt in full and never accumulate small files."""
    assert all(table.startswith(("bronze.", "silver.")) for table in DEFAULT_TABLES)
    assert "bronze.trades" in DEFAULT_TABLES


class _FakeSnapshot:
    def __init__(self, snapshot_id: int, timestamp_ms: int) -> None:
        self.snapshot_id = snapshot_id
        self.timestamp_ms = timestamp_ms

    def manifests(self, _io: object) -> list[object]:
        return []


class _FakeMetadata:
    def __init__(self, snapshots: list[_FakeSnapshot]) -> None:
        self.snapshots = snapshots


class _FakeTable:
    def __init__(self, snapshots: list[_FakeSnapshot]) -> None:
        self.metadata = _FakeMetadata(snapshots)
        self.io = object()
        self.rewrote_manifests = False

    def current_snapshot(self) -> _FakeSnapshot | None:
        return self.metadata.snapshots[-1] if self.metadata.snapshots else None

    def rewrite_manifests(self) -> None:
        self.rewrote_manifests = True


class _FakeCatalog:
    def __init__(self, table: _FakeTable) -> None:
        self._table = table

    def load_table(self, _identifier: str) -> _FakeTable:
        return self._table


def test_dry_run_commits_nothing() -> None:
    """A dry run must be safe to point at production while you decide."""
    table = _FakeTable([_FakeSnapshot(1, 0), _FakeSnapshot(2, 0)])
    maintenance = TableMaintenance(_FakeCatalog(table), dry_run=True)

    maintenance.rewrite_manifests("bronze.trades")
    maintenance.expire_snapshots("bronze.trades")

    assert table.rewrote_manifests is False
    assert len(table.metadata.snapshots) == 2


def test_a_failing_step_is_recorded_and_the_pass_continues() -> None:
    """One broken table must not abandon the other six."""

    class _ExplodingCatalog(_FakeCatalog):
        def load_table(self, identifier: str) -> _FakeTable:
            raise RuntimeError("catalog unreachable")

    report = TableMaintenance(_ExplodingCatalog(_FakeTable([]))).run("bronze.trades")
    assert not report.ok
    assert "inspect failed" in report.errors[0]


def test_empty_table_reports_zero_files_rather_than_failing() -> None:
    maintenance = TableMaintenance(_FakeCatalog(_FakeTable([])))
    snapshots, files = maintenance.inspect("bronze.trades")
    assert (snapshots, files) == (0, 0)
