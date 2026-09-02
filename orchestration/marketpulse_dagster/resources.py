"""Resources: the platform's external systems, declared once.

Every asset takes its dependencies as resources rather than constructing them,
which is what lets the same asset code run against the compose stack, against
a test double, and against a production deployment without a branch anywhere in
the asset body.
"""

from __future__ import annotations

import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from dagster import ConfigurableResource, EnvVar, get_dagster_logger
from pydantic import Field, PrivateAttr

__all__ = [
    "IcebergCatalogResource",
    "MarketPulseCliResource",
    "SparkSubmitResource",
    "TrinoResource",
]


class MarketPulseCliResource(ConfigurableResource):
    """Runs the ingestion CLI as a subprocess.

    Shelling out rather than importing looks crude and is deliberate. The CLI
    is the interface an engineer uses at 3am, so making the orchestrator use
    the identical entry point means a Dagster failure is reproducible by
    copying one command out of the logs. Importing the functions instead would
    create a second invocation path that drifts.
    """

    executable: str = "marketpulse"
    working_directory: str = "/opt/dagster/app"
    timeout_seconds: int = 3_600

    def run(self, *args: str) -> str:
        """Execute a subcommand, streaming output into the Dagster log."""
        logger = get_dagster_logger()
        command = [self.executable, *args]
        logger.info("running: %s", " ".join(command))

        completed = subprocess.run(  # noqa: S603 - fixed executable, no shell
            command,
            cwd=self.working_directory,
            capture_output=True,
            text=True,
            timeout=self.timeout_seconds,
            check=False,
        )
        if completed.stdout:
            logger.info(completed.stdout.strip())
        if completed.returncode != 0:
            logger.error(completed.stderr.strip())
            raise RuntimeError(
                f"`{' '.join(command)}` exited {completed.returncode}: "
                f"{completed.stderr.strip()[:2000]}"
            )
        return completed.stdout


class SparkSubmitResource(ConfigurableResource):
    """Submits a Spark job into the Spark container.

    Fire-and-forget for the long-running streaming queries (they outlive the
    Dagster run that started them) and blocking for batch jobs.
    """

    container: str = "mp-spark"
    spark_submit: str = "spark-submit"
    timeout_seconds: int = 7_200

    def submit(self, script: str, *args: str, detach: bool = False) -> str:
        logger = get_dagster_logger()
        command = [
            "docker",
            "exec",
            *(["-d"] if detach else ["-T"]),
            self.container,
            self.spark_submit,
            script,
            *args,
        ]
        logger.info("submitting: %s", " ".join(command))
        completed = subprocess.run(  # noqa: S603
            command, capture_output=True, text=True, timeout=self.timeout_seconds, check=False
        )
        if completed.returncode != 0:
            raise RuntimeError(f"spark-submit failed: {completed.stderr[:2000]}")
        return completed.stdout


class TrinoResource(ConfigurableResource):
    """A thin Trino query interface for the asset checks.

    Deliberately not a full ORM. The checks need to run a scalar query and read
    one number; anything more sophisticated belongs in dbt where it is version
    controlled and tested.
    """

    host: str = Field(default_factory=lambda: EnvVar("MP_TRINO_HOST").get_value("localhost"))
    port: int = 8080
    user: str = "dagster"
    catalog: str = "lakehouse"
    schema_: str = Field(default="silver", alias="schema")

    _connection: Any = PrivateAttr(default=None)

    def _connect(self) -> Any:
        import trino  # noqa: PLC0415

        return trino.dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema_,
        )

    def query(self, sql: str) -> list[tuple[Any, ...]]:
        connection = self._connect()
        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            return list(cursor.fetchall())
        finally:
            connection.close()

    def scalar(self, sql: str, default: Any = None) -> Any:
        """Run a query expected to return one row and one column."""
        rows = self.query(sql)
        if not rows or rows[0][0] is None:
            return default
        return rows[0][0]


class IcebergCatalogResource(ConfigurableResource):
    """PyIceberg handle, used by the maintenance assets.

    Maintenance runs through PyIceberg rather than Spark because expiring
    snapshots and rewriting manifests does not need a cluster, and starting one
    to do it turns a two-second operation into a two-minute one.
    """

    uri: str = "http://iceberg-rest:8181"
    warehouse: str = "s3://lakehouse/warehouse"
    s3_endpoint: str = "http://minio:9000"
    access_key: str = "minioadmin"
    # The MinIO development default, not a secret. A real deployment overrides
    # both from the environment; nothing here is ever baked into an image.
    secret_key: str = "minioadmin"  # noqa: S105

    def load(self) -> Any:
        from pyiceberg.catalog import load_catalog  # noqa: PLC0415

        return load_catalog(
            "lakehouse",
            **{
                "uri": self.uri,
                "warehouse": self.warehouse,
                "s3.endpoint": self.s3_endpoint,
                "s3.access-key-id": self.access_key,
                "s3.secret-access-key": self.secret_key,
                "s3.path-style-access": "true",
            },
        )

    def tables(self, namespaces: Iterator[str] | list[str] | None = None) -> list[str]:
        catalog = self.load()
        names: list[str] = []
        for namespace in namespaces or ["bronze", "silver", "gold", "ops"]:
            names.extend(f"{namespace}.{name}" for _, name in catalog.list_tables(namespace))
        return names


def dbt_project_dir() -> Path:
    """Resolve the dbt project directory, container path first, repo path second."""
    container = Path("/opt/dagster/dbt/marketpulse")
    if container.is_dir():
        return container
    return Path(__file__).resolve().parents[2] / "dbt" / "marketpulse"
