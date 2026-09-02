"""Software-defined assets for the MarketPulse platform."""

from marketpulse_dagster.assets.bronze import bronze_reference_load, bronze_streaming_queries
from marketpulse_dagster.assets.ingestion import (
    binance_klines_backfill,
    fx_reference_rates,
    instrument_metadata,
)
from marketpulse_dagster.assets.maintenance import (
    compact_iceberg_tables,
    expire_iceberg_snapshots,
    remove_orphan_files,
)
from marketpulse_dagster.assets.transformations import marketpulse_dbt_assets

__all__ = [
    "binance_klines_backfill",
    "bronze_reference_load",
    "bronze_streaming_queries",
    "compact_iceberg_tables",
    "expire_iceberg_snapshots",
    "fx_reference_rates",
    "instrument_metadata",
    "marketpulse_dbt_assets",
    "remove_orphan_files",
]
