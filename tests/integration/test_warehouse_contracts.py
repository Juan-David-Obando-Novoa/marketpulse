"""Assertions about the deployed warehouse itself.

These check the properties that only exist once the tables are real: that the
layer boundaries hold in the catalog, that the partition specs are what the DDL
intended, and that a query the serving layer issues actually planned.
"""

from __future__ import annotations

from typing import Any

import pytest

pytestmark = pytest.mark.integration

EXPECTED_BRONZE = {"trades", "book_ticker", "klines", "fx_rates", "instrument_metadata"}
EXPECTED_GOLD = {
    "dim_instrument",
    "fct_market_1m",
    "fct_kline_reconciliation",
    "mart_symbol_daily",
    "mart_pipeline_health",
    "mart_liquidity_ranking",
}


def _tables(connection: Any, schema: str) -> set[str]:
    cursor = connection.cursor()
    cursor.execute(f"show tables from lakehouse.{schema}")
    return {row[0] for row in cursor.fetchall()}


def test_bronze_namespace_matches_the_ddl(trino_connection: Any) -> None:
    assert EXPECTED_BRONZE <= _tables(trino_connection, "bronze")


def test_gold_namespace_is_complete(trino_connection: Any) -> None:
    assert EXPECTED_GOLD <= _tables(trino_connection, "gold")


def test_bronze_trades_is_partitioned_as_designed(trino_connection: Any) -> None:
    """A table created by an inferred write would have no partition spec at all."""
    cursor = trino_connection.cursor()
    cursor.execute('select * from lakehouse.bronze."trades$partitions" limit 1')
    columns = {description[0].lower() for description in cursor.description}
    assert any("trade_time" in column for column in columns), columns
    assert any("symbol" in column for column in columns), columns


def test_time_travel_is_available_within_the_retention_window(trino_connection: Any) -> None:
    """Time travel is a promise ADR-0002 makes; this is the promise being kept."""
    cursor = trino_connection.cursor()
    cursor.execute('select count(*) from lakehouse.bronze."trades$snapshots"')
    assert cursor.fetchone()[0] >= 1


def test_gold_marts_have_no_duplicate_grain(trino_connection: Any) -> None:
    """The grain assertion, run against the deployed table rather than a dbt model."""
    cursor = trino_connection.cursor()
    cursor.execute(
        """
        select count(*)
        from (
            select symbol, bar_start, count(*) as n
            from lakehouse.gold.fct_market_1m
            group by 1, 2
            having count(*) > 1
        )
        """
    )
    assert cursor.fetchone()[0] == 0


def test_fx_intervals_never_overlap(trino_connection: Any) -> None:
    """The highest-consequence invariant in the warehouse, checked in place.

    An overlap here multiplies rows in every range join that touches the table.
    """
    cursor = trino_connection.cursor()
    cursor.execute(
        """
        with intervals as (
            select
                currency_pair,
                valid_to,
                lead(valid_from) over (partition by currency_pair order by valid_from) as next_from
            from lakehouse.silver.slv_fx_rates
        )
        select count(*) from intervals where next_from is not null and valid_to > next_from
        """
    )
    assert cursor.fetchone()[0] == 0
