"""Jobs: named selections of the asset graph.

Jobs exist here only where a *subset* needs its own schedule or its own retry
behaviour. Everything else is materialised through the asset graph directly,
which is the point of the asset model -- a job per asset would be re-inventing
the task DAG that ADR-0005 chose against.
"""

from __future__ import annotations

from dagster import AssetSelection, RetryPolicy, define_asset_job

__all__ = [
    "daily_ingestion_job",
    "maintenance_job",
    "streaming_supervision_job",
    "warehouse_build_job",
]

#: Ingestion: the partitioned assets that pull from the venue each day.
daily_ingestion_job = define_asset_job(
    name="daily_ingestion",
    selection=AssetSelection.groups("ingestion") | AssetSelection.keys("bronze_reference_load"),
    description=(
        "Fetches the previous day's candles, reference rates and instrument "
        "metadata, then drains them into bronze."
    ),
    # Retried at the job level as well as per asset: a transient Trino restart
    # takes out the whole run rather than one step.
    op_retry_policy=RetryPolicy(max_retries=2, delay=60),
)

#: The warehouse: everything dbt owns, plus the checks attached to it.
warehouse_build_job = define_asset_job(
    name="warehouse_build",
    selection=(
        AssetSelection.groups("staging", "silver", "gold")
        # include_sources False: the sources are produced by the ingestion job
        # and re-materialising them here would run the backfill twice.
        .required_multi_asset_neighbors()
    ),
    description="dbt build across staging, silver and gold, with the asset checks.",
)

#: Streaming supervision. Runs often and does almost nothing -- its value is
#: that a stopped query is noticed in minutes rather than the next morning.
streaming_supervision_job = define_asset_job(
    name="streaming_supervision",
    selection=AssetSelection.keys("bronze_streaming_queries"),
    description="Confirms the Kafka-to-Iceberg queries are alive and landing rows.",
)

#: Maintenance. Separate from everything else because it is the one job that is
#: safe to skip for a day and expensive to run during peak load.
maintenance_job = define_asset_job(
    name="iceberg_maintenance",
    selection=AssetSelection.groups("maintenance"),
    description="Compact, then expire snapshots, then remove orphans. In that order.",
)
