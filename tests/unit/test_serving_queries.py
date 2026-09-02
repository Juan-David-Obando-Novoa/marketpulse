"""Serving-layer SQL.

These tests do not run SQL. They assert the two properties that make the
serving layer safe regardless of what it is pointed at: every statement is
parameterised, and every result is bounded server-side.
"""

from __future__ import annotations

import pytest

from marketpulse.serving import queries

pytestmark = pytest.mark.unit

ALL_QUERIES = [
    queries.instruments(tracked_only=True),
    queries.candles(symbol="BTCUSDT", start="2026-03-14T00:00:00", end="2026-03-15T00:00:00", limit=100),
    queries.liquidity_ranking(window_days=7, limit=50),
    queries.quality_report(symbol="BTCUSDT", days=7),
]


@pytest.mark.parametrize("query", ALL_QUERIES, ids=lambda q: q.sql.split()[0] + str(len(q.sql)))
def test_every_query_is_parameterised(query: queries.Query) -> None:
    """No interpolated values, ever.

    Not because these endpoints are internet-facing today, but because a
    string-formatted query is one refactor away from being reachable from
    somewhere that is, and by then nobody remembers which of forty statements
    was safe.
    """
    assert query.params, "query has no bound parameters"
    assert query.sql.count("?") == len(query.params)


@pytest.mark.parametrize("query", ALL_QUERIES, ids=range(len(ALL_QUERIES)))
def test_every_query_is_read_only(query: queries.Query) -> None:
    lowered = query.sql.lower()
    for verb in ("insert ", "update ", "delete ", "drop ", "alter ", "create ", "merge "):
        assert verb not in lowered, f"serving query contains {verb.strip()}"


@pytest.mark.parametrize("query", ALL_QUERIES, ids=range(len(ALL_QUERIES)))
def test_every_query_reads_gold_only(query: queries.Query) -> None:
    """The serving layer is a consumer. Reading silver would bypass the marts."""
    lowered = query.sql.lower()
    assert "lakehouse.bronze." not in lowered
    assert "lakehouse.silver." not in lowered


def test_candle_limit_is_capped_server_side() -> None:
    """A caller asking for a million bars gets the ceiling, not a million bars."""
    query = queries.candles(
        symbol="BTCUSDT", start="2026-03-14T00:00:00", end="2026-03-15T00:00:00", limit=10**9
    )
    assert query.params[-1] == queries.MAX_PAGE_SIZE


def test_ranking_limit_is_capped_server_side() -> None:
    assert queries.liquidity_ranking(window_days=7, limit=10**6).params[-1] == 200


def test_candle_window_is_half_open() -> None:
    """Adjacent requests must tile: no duplicated bar at a page boundary."""
    sql = queries.candles(
        symbol="BTCUSDT", start="2026-03-14T00:00:00", end="2026-03-15T00:00:00", limit=10
    ).sql.lower()
    assert "bar_start >= from_iso8601_timestamp(?)" in sql
    assert "bar_start <  from_iso8601_timestamp(?)" in sql or "bar_start < from_iso8601_timestamp(?)" in sql
