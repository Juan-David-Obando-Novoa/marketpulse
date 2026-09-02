"""Typed, environment-driven configuration for every MarketPulse component.

Configuration is read once, validated once, and passed explicitly. Nothing in
this codebase reads ``os.environ`` outside this module: that keeps the set of
knobs discoverable and makes every component trivially testable by constructing
a settings object rather than mutating the process environment.

Environment variables are namespaced per concern (``MP_KAFKA__``, ``MP_S3__``,
...) using pydantic-settings' nested delimiter, which means the compose file
and a production secret store can share one naming convention.
"""

from __future__ import annotations

from enum import Enum
from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = [
    "AppSettings",
    "BinanceSettings",
    "Environment",
    "FxSettings",
    "IcebergSettings",
    "KafkaSettings",
    "ObservabilitySettings",
    "S3Settings",
    "get_settings",
]


class Environment(str, Enum):
    """Deployment environment. Drives log formatting and safety rails."""

    LOCAL = "local"
    CI = "ci"
    STAGING = "staging"
    PRODUCTION = "production"


class KafkaSettings(BaseModel):
    """Connection and delivery-semantics settings for the Kafka/Redpanda tier.

    The producer defaults encode ADR-0007: idempotent, ``acks=all``, infinite
    retries bounded by a delivery timeout rather than a retry count, and
    compression on because market-data payloads are highly repetitive JSON.
    """

    bootstrap_servers: str = "localhost:19092"
    schema_registry_url: str = "http://localhost:18081"
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: SecretStr | None = None

    client_id: str = "marketpulse-producer"
    compression_type: str = "zstd"
    linger_ms: int = Field(default=50, ge=0, le=10_000)
    batch_size: int = Field(default=1_048_576, ge=1_024)
    enable_idempotence: bool = True
    acks: str = "all"
    max_in_flight: int = Field(default=5, ge=1, le=5)
    delivery_timeout_ms: int = Field(default=120_000, ge=1_000)
    queue_buffering_max_messages: int = Field(default=200_000, ge=1_000)

    topic_trades: str = "md.trades.v1"
    topic_book_ticker: str = "md.book_ticker.v1"
    topic_klines: str = "md.klines.v1"
    topic_fx_rates: str = "ref.fx_rates.v1"
    topic_dlq: str = "md.dead_letter.v1"

    @property
    def topics(self) -> dict[str, str]:
        """Logical stream name -> physical topic name."""
        return {
            "trades": self.topic_trades,
            "book_ticker": self.topic_book_ticker,
            "klines": self.topic_klines,
            "fx_rates": self.topic_fx_rates,
            "dead_letter": self.topic_dlq,
        }

    def producer_config(self) -> dict[str, object]:
        """Render a ``confluent_kafka.Producer`` configuration dictionary."""
        config: dict[str, object] = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "security.protocol": self.security_protocol,
            "compression.type": self.compression_type,
            "linger.ms": self.linger_ms,
            "batch.size": self.batch_size,
            "enable.idempotence": self.enable_idempotence,
            "acks": self.acks,
            "max.in.flight.requests.per.connection": self.max_in_flight,
            "delivery.timeout.ms": self.delivery_timeout_ms,
            "queue.buffering.max.messages": self.queue_buffering_max_messages,
        }
        if self.sasl_mechanism:
            config["sasl.mechanism"] = self.sasl_mechanism
            config["sasl.username"] = self.sasl_username or ""
            config["sasl.password"] = (
                self.sasl_password.get_secret_value() if self.sasl_password else ""
            )
        return config


class S3Settings(BaseModel):
    """Object-storage settings. MinIO locally, real S3 in a cloud deployment."""

    endpoint: str = "http://localhost:9000"
    region: str = "us-east-1"
    access_key: SecretStr = SecretStr("minioadmin")
    secret_key: SecretStr = SecretStr("minioadmin")
    bucket: str = "lakehouse"
    path_style_access: bool = True

    @property
    def warehouse_uri(self) -> str:
        return f"s3://{self.bucket}/warehouse"

    @property
    def checkpoint_uri(self) -> str:
        return f"s3a://{self.bucket}/checkpoints"


class IcebergSettings(BaseModel):
    """Iceberg REST catalog coordinates and the layer namespaces."""

    catalog_name: str = "lakehouse"
    rest_uri: str = "http://localhost:8181"
    bronze_namespace: str = "bronze"
    silver_namespace: str = "silver"
    gold_namespace: str = "gold"

    #: Target size for rewritten data files, in bytes (see maintenance jobs).
    target_file_size_bytes: int = Field(default=134_217_728, ge=8 * 1024 * 1024)
    #: Snapshots older than this are expired nightly.
    snapshot_retention_days: int = Field(default=7, ge=1)

    def table(self, namespace: str, name: str) -> str:
        return f"{self.catalog_name}.{namespace}.{name}"


class BinanceSettings(BaseModel):
    """Public market-data endpoints.

    Defaults point at ``*.binance.vision``, the read-only public market-data
    mirror: it needs no API key, is not subject to the trading endpoints'
    geographic restrictions, and is the correct target for an analytics
    workload that never places an order.
    """

    ws_base_url: str = "wss://data-stream.binance.vision/stream"
    rest_base_url: str = "https://data-api.binance.vision"
    symbols: list[str] = Field(default_factory=lambda: ["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    streams: list[str] = Field(default_factory=lambda: ["trade", "bookTicker"])

    #: Seconds without a message before the socket is considered dead.
    idle_timeout_seconds: float = Field(default=30.0, gt=0)
    ping_interval_seconds: float = Field(default=20.0, gt=0)
    reconnect_initial_backoff_seconds: float = Field(default=1.0, gt=0)
    reconnect_max_backoff_seconds: float = Field(default=60.0, gt=0)
    rest_max_requests_per_minute: int = Field(default=1_000, ge=1)

    @field_validator("symbols")
    @classmethod
    def _upper_and_unique(cls, value: list[str]) -> list[str]:
        seen: list[str] = []
        for symbol in value:
            normalised = symbol.strip().upper()
            if normalised and normalised not in seen:
                seen.append(normalised)
        if not seen:
            raise ValueError("at least one symbol must be configured")
        return seen

    @property
    def stream_names(self) -> list[str]:
        """Combined-stream identifiers, e.g. ``btcusdt@trade``."""
        return [f"{s.lower()}@{stream}" for s in self.symbols for stream in self.streams]


class FxSettings(BaseModel):
    """Reference FX feed: the Colombian TRM published as a Socrata dataset.

    Market data is quoted in USD; a Colombian consumer needs COP. The TRM is
    the official rate, published daily by the Banco de la Republica through
    the national open-data portal.
    """

    socrata_base_url: str = "https://www.datos.gov.co/resource"
    trm_dataset_id: str = "mcec-87by"
    app_token: SecretStr | None = None
    page_size: int = Field(default=1_000, ge=1, le=50_000)


class ObservabilitySettings(BaseModel):
    metrics_port: int = Field(default=9108, ge=1, le=65_535)
    metrics_enabled: bool = True
    log_level: str = "INFO"
    json_logs: bool = True
    openlineage_url: str | None = None
    openlineage_namespace: str = "marketpulse"


class AppSettings(BaseSettings):
    """Root settings object. Instantiate once via :func:`get_settings`."""

    model_config = SettingsConfigDict(
        env_prefix="MP_",
        env_nested_delimiter="__",
        env_file=(".env", ".env.local"),
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    environment: Environment = Environment.LOCAL
    service_name: str = "marketpulse"
    project_root: Path = Field(default_factory=lambda: Path(__file__).resolve().parents[2])

    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    s3: S3Settings = Field(default_factory=S3Settings)
    iceberg: IcebergSettings = Field(default_factory=IcebergSettings)
    binance: BinanceSettings = Field(default_factory=BinanceSettings)
    fx: FxSettings = Field(default_factory=FxSettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)

    @property
    def is_production(self) -> bool:
        return self.environment is Environment.PRODUCTION


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    """Return the process-wide settings singleton.

    Cached so that repeated imports do not re-read the environment. Tests that
    need different settings construct :class:`AppSettings` directly instead of
    clearing this cache, which keeps them independent of import order.
    """
    return AppSettings()
