"""dbt assets.

``@dbt_assets`` loads the dbt manifest and turns every model, seed, snapshot
and test into a Dagster asset. That is the entire argument for Dagster over
Airflow in ADR-0005: dbt's DAG and the platform's DAG become one graph, so
Dagster knows that ``fct_market_1m`` depends on ``slv_ohlcv_1m`` which depends
on the Kafka topic, without anyone restating those edges.

The translator below is what makes the two halves line up: it maps dbt's
schema names onto Dagster groups and wires the dbt sources to the upstream
Python assets, so the graph is connected rather than two islands that happen to
share a UI.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

from dagster import AssetExecutionContext, AssetKey, BackfillPolicy, Output
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

from marketpulse_dagster.resources import dbt_project_dir

__all__ = ["MarketPulseDbtTranslator", "marketpulse_dbt_assets"]

DBT_PROJECT_DIR = dbt_project_dir()
DBT_MANIFEST = DBT_PROJECT_DIR / "target" / "manifest.json"


class MarketPulseDbtTranslator(DagsterDbtTranslator):
    """Aligns dbt's naming with the platform's asset graph."""

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        """Map dbt sources onto the Python assets that actually produce them.

        Without this, ``source.bronze.trades`` and the streaming asset that
        writes it are two unrelated nodes, and the lineage stops at the
        warehouse boundary -- which is exactly the boundary anyone debugging a
        stale mart needs to cross.
        """
        if dbt_resource_props["resource_type"] == "source":
            source_name = dbt_resource_props["source_name"]
            table_name = dbt_resource_props["name"]
            if source_name == "bronze" and table_name in ("trades", "book_ticker"):
                return AssetKey("bronze_streaming_queries")
            if source_name == "bronze":
                return AssetKey("bronze_reference_load")
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        """Group by lake layer, so the UI reads the way the architecture does."""
        fqn = dbt_resource_props.get("fqn", [])
        for layer in ("staging", "silver", "gold"):
            if layer in fqn:
                return layer
        return dbt_resource_props.get("resource_type", "dbt")

    def get_description(self, dbt_resource_props: Mapping[str, Any]) -> str:
        """Carry the dbt description through, so documentation is written once."""
        return dbt_resource_props.get("description") or super().get_description(dbt_resource_props)


@dbt_assets(
    manifest=DBT_MANIFEST,
    dagster_dbt_translator=MarketPulseDbtTranslator(),
    # Backfilling the warehouse one partition at a time would serialise
    # hundreds of small Trino queries. dbt's own incremental predicates already
    # reconsider a lookback window, so a single run covers a range correctly.
    backfill_policy=BackfillPolicy.single_run(),
)
def marketpulse_dbt_assets(
    context: AssetExecutionContext, dbt: DbtCliResource
) -> Iterator[Output[Any]]:
    """Run `dbt build`: models, seeds, snapshots and tests in dependency order.

    ``build`` rather than ``run`` then ``test`` deliberately. build interleaves
    them, so a model whose upstream test failed is not built at all -- which is
    the difference between a broken table being caught and a broken table being
    published and then reported on.
    """
    yield from dbt.cli(["build"], context=context).stream()
