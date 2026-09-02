"""Fixtures for tests that need the compose stack.

These are skipped rather than failed when the stack is not up. A developer
running `pytest` on a laptop with nothing running should see a clean pass and a
skip count, not fourteen connection errors -- otherwise the unit suite stops
being run at all.
"""

from __future__ import annotations

import os
import socket
from collections.abc import Iterator
from typing import Any

import pytest


def _reachable(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _require(host: str, port: int, name: str) -> None:
    if not _reachable(host, port):
        pytest.skip(f"{name} is not reachable at {host}:{port}; run `make up` first")


@pytest.fixture(scope="session")
def kafka_bootstrap() -> str:
    host, port = os.getenv("MP_KAFKA__BOOTSTRAP_SERVERS", "localhost:19092").split(":")
    _require(host, int(port), "Redpanda")
    return f"{host}:{port}"


@pytest.fixture(scope="session")
def schema_registry_url() -> str:
    url = os.getenv("MP_KAFKA__SCHEMA_REGISTRY_URL", "http://localhost:18081")
    host, port = url.removeprefix("http://").split(":")
    _require(host, int(port), "Schema Registry")
    return url


@pytest.fixture(scope="session")
def trino_connection() -> Iterator[Any]:
    host = os.getenv("MP_TRINO_HOST", "localhost")
    port = int(os.getenv("MP_TRINO_PORT", "8090"))
    _require(host, port, "Trino")

    import trino  # noqa: PLC0415

    connection = trino.dbapi.connect(host=host, port=port, user="pytest", catalog="lakehouse")
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture(scope="session")
def iceberg_rest_uri() -> str:
    uri = os.getenv("MP_ICEBERG__REST_URI", "http://localhost:8181")
    host, port = uri.removeprefix("http://").split(":")
    _require(host, int(port), "Iceberg REST catalog")
    return uri
