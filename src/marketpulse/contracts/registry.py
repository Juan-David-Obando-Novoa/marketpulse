"""Schema loading, registry interaction and Confluent wire-format codecs.

Two deliberate decisions live in this module.

**We implement the Confluent wire format ourselves** (magic byte ``0x00``, a
four-byte big-endian schema id, then the Avro body) on top of fastavro rather
than using ``confluent_kafka.schema_registry.AvroSerializer``. The reason is
symmetry: Spark's ``from_avro`` has no notion of the registry, so the streaming
job has to strip those five bytes by hand anyway. Owning both ends means the
framing is defined once, in one place, and is unit-testable without a broker.

**Compatibility is enforced in CI, not at runtime.** ``check_compatibility``
is called by the pipeline before merge. A producer that discovers at 3am that
its schema is incompatible has already failed; the useful place to fail is the
pull request.
"""

from __future__ import annotations

import io
import json
import struct
import urllib.error
import urllib.request
from functools import lru_cache
from pathlib import Path
from typing import Any

import fastavro

__all__ = [
    "MAGIC_BYTE",
    "AvroCodec",
    "SchemaRegistryClient",
    "SchemaRegistryError",
    "list_schema_files",
    "load_schema",
    "subject_for",
]

#: Confluent framing: one magic byte then a big-endian int32 schema id.
MAGIC_BYTE = 0
_HEADER = struct.Struct(">bI")
_SCHEMA_DIR = Path(__file__).parent / "schemas"


class SchemaRegistryError(RuntimeError):
    """Raised when the registry rejects a request or is unreachable."""


@lru_cache(maxsize=32)
def load_schema(filename: str) -> dict[str, Any]:
    """Load and parse an Avro schema shipped with the package.

    Cached because the streaming jobs load schemas per micro-batch and parsing
    JSON forty times a minute for no reason is the kind of thing that shows up
    in a flame graph six months later.
    """
    path = _SCHEMA_DIR / filename
    if not path.is_file():
        raise FileNotFoundError(f"no such Avro schema: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def list_schema_files() -> list[str]:
    """Every ``.avsc`` shipped with the package, sorted for deterministic CI."""
    return sorted(p.name for p in _SCHEMA_DIR.glob("*.avsc"))


def subject_for(topic: str, *, is_key: bool = False) -> str:
    """Registry subject name under the default TopicNameStrategy.

    TopicNameStrategy is chosen over RecordNameStrategy because our topics are
    single-type by design; the moment a topic carries a union of record types,
    per-topic compatibility checking stops meaning anything useful.
    """
    return f"{topic}-{'key' if is_key else 'value'}"


class AvroCodec:
    """Encode and decode a single Avro schema in Confluent wire format.

    ``schema_id`` may be ``None``, in which case the codec reads and writes a
    bare Avro body. That mode exists for tests and for the Spark side, which
    receives already-stripped bytes.
    """

    __slots__ = ("_parsed", "_schema", "_schema_id")

    def __init__(self, schema: dict[str, Any], schema_id: int | None = None) -> None:
        self._schema = schema
        self._parsed = fastavro.parse_schema(schema)
        self._schema_id = schema_id

    @classmethod
    def from_file(cls, filename: str, schema_id: int | None = None) -> AvroCodec:
        return cls(load_schema(filename), schema_id)

    @property
    def schema(self) -> dict[str, Any]:
        return self._schema

    @property
    def schema_id(self) -> int | None:
        return self._schema_id

    def with_schema_id(self, schema_id: int) -> AvroCodec:
        """Return a copy bound to a registry-assigned id."""
        return AvroCodec(self._schema, schema_id)

    def encode(self, record: dict[str, Any]) -> bytes:
        """Serialise ``record``, prefixing the Confluent header when bound."""
        buffer = io.BytesIO()
        if self._schema_id is not None:
            buffer.write(_HEADER.pack(MAGIC_BYTE, self._schema_id))
        fastavro.schemaless_writer(buffer, self._parsed, record)
        return buffer.getvalue()

    def decode(self, payload: bytes) -> dict[str, Any]:
        """Deserialise ``payload``, tolerating both framed and bare bodies."""
        body = strip_confluent_header(payload) if is_framed(payload) else payload
        return fastavro.schemaless_reader(io.BytesIO(body), self._parsed)  # type: ignore[return-value]


def is_framed(payload: bytes) -> bool:
    """True when ``payload`` carries the Confluent five-byte header."""
    return len(payload) > _HEADER.size and payload[0] == MAGIC_BYTE


def strip_confluent_header(payload: bytes) -> bytes:
    """Drop the magic byte and schema id, returning the bare Avro body.

    Mirrors what the Spark job does with ``substring(value, 6, ...)`` before
    calling ``from_avro``; see ``streaming/common.py``.
    """
    if not is_framed(payload):
        raise ValueError("payload is not in Confluent wire format")
    return payload[_HEADER.size :]


def schema_id_of(payload: bytes) -> int:
    """Read the registry schema id out of a framed payload."""
    if not is_framed(payload):
        raise ValueError("payload is not in Confluent wire format")
    _, schema_id = _HEADER.unpack(payload[: _HEADER.size])
    return int(schema_id)


class SchemaRegistryClient:
    """Minimal Schema Registry client covering register, fetch and compatibility.

    Intentionally small and stdlib-only: it is used by the producer at start-up
    and by CI, both of which need three endpoints and no connection pooling.
    """

    def __init__(self, base_url: str, *, timeout: float = 10.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    def _request(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        url = f"{self._base_url}{path}"
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(  # noqa: S310 - fixed http(s) registry URL
            url,
            data=data,
            method=method,
            headers={
                "Content-Type": "application/vnd.schemaregistry.v1+json",
                "Accept": "application/vnd.schemaregistry.v1+json, application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:  # noqa: S310
                return json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as exc:  # pragma: no cover - network path
            detail = exc.read().decode(errors="replace")
            raise SchemaRegistryError(f"{method} {url} -> {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:  # pragma: no cover - network path
            raise SchemaRegistryError(f"{method} {url} unreachable: {exc.reason}") from exc

    def register(self, subject: str, schema: dict[str, Any]) -> int:
        """Register ``schema`` under ``subject`` and return its global id."""
        payload = self._request(
            "POST",
            f"/subjects/{subject}/versions",
            {"schema": json.dumps(schema), "schemaType": "AVRO"},
        )
        return int(payload["id"])

    def latest(self, subject: str) -> dict[str, Any]:
        return self._request("GET", f"/subjects/{subject}/versions/latest")

    def check_compatibility(self, subject: str, schema: dict[str, Any]) -> bool:
        """True when ``schema`` may replace the latest version of ``subject``.

        A subject that does not exist yet is trivially compatible: the first
        version of a contract cannot break a reader that does not exist.
        """
        try:
            payload = self._request(
                "POST",
                f"/compatibility/subjects/{subject}/versions/latest?verbose=true",
                {"schema": json.dumps(schema), "schemaType": "AVRO"},
            )
        except SchemaRegistryError as exc:
            if "40401" in str(exc) or ("Subject" in str(exc) and "not found" in str(exc)):
                return True
            raise
        return bool(payload.get("is_compatible", False))

    def set_compatibility(self, subject: str, level: str = "BACKWARD") -> None:
        """Pin the compatibility level for a subject (ADR-0006)."""
        self._request("PUT", f"/config/{subject}", {"compatibility": level})
