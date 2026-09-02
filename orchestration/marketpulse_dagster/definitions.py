"""The Dagster code location.

Everything the platform knows how to do, and every external system it needs,
resolved in one object. Resources are configured from the environment here and
only here, mirroring the rule in ``marketpulse.config``: one place reads the
environment, everything else is handed what it needs.
"""

from __future__ import annotations

import os

from dagster import Definitions, EnvVar, load_assets_from_modules
from dagster_dbt import DbtCliResource

from marketpulse_dagster import checks
from marketpulse_dagster.assets import bronze, ingestion, maintenance, transformations
from marketpulse_dagster.jobs import (
    daily_ingestion_job,
    maintenance_job,
    streaming_supervision_job,
    warehouse_build_job,
)
from marketpulse_dagster.resources import (
    IcebergCatalogResource,
    MarketPulseCliResource,
    SparkSubmitResource,
    TrinoResource,
    dbt_project_dir,
)
from marketpulse_dagster.schedules import (
    daily_ingestion_schedule,
    maintenance_schedule,
    streaming_supervision_schedule,
    warehouse_build_schedule,
)
from marketpulse_dagster.sensors import (
    bronze_landed_sensor,
    pipeline_failure_sensor,
    stalled_stream_sensor,
)

all_assets = load_assets_from_modules([ingestion, bronze, maintenance, transformations])

all_checks = [
    checks.check_silver_freshness,
    checks.check_trade_volume_within_baseline,
    checks.check_reconciliation_against_venue,
    checks.check_no_decode_quarantine,
    checks.check_all_tracked_symbols_present,
]

defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    jobs=[
        daily_ingestion_job,
        warehouse_build_job,
        streaming_supervision_job,
        maintenance_job,
    ],
    schedules=[
        daily_ingestion_schedule,
        warehouse_build_schedule,
        streaming_supervision_schedule,
        maintenance_schedule,
    ],
    sensors=[
        bronze_landed_sensor,
        stalled_stream_sensor,
        pipeline_failure_sensor,
    ],
    resources={
        "marketpulse_cli": MarketPulseCliResource(
            executable=os.getenv("MP_CLI", "marketpulse"),
        ),
        "spark": SparkSubmitResource(
            container=os.getenv("MP_SPARK_CONTAINER", "mp-spark"),
        ),
        "trino": TrinoResource(
            host=EnvVar("MP_TRINO_HOST").get_value("trino"),
            port=int(os.getenv("MP_TRINO_PORT", "8080")),
        ),
        "iceberg": IcebergCatalogResource(
            uri=EnvVar("MP_ICEBERG__REST_URI").get_value("http://iceberg-rest:8181"),
            s3_endpoint=EnvVar("MP_S3__ENDPOINT").get_value("http://minio:9000"),
        ),
        "dbt": DbtCliResource(
            project_dir=str(dbt_project_dir()),
            profiles_dir=str(dbt_project_dir()),
            target=os.getenv("DBT_TARGET", "local"),
        ),
    },
)
