"""Operator entry points.

Every long-running or one-shot ingestion action is a subcommand here, and the
Dagster assets shell out to nothing else: the orchestrator calls the same code
paths an engineer runs by hand at 3am. That symmetry is deliberate. A pipeline
whose only invocation path is the orchestrator is a pipeline you cannot debug.

    marketpulse stream                      # live websocket feed
    marketpulse backfill --symbol BTCUSDT --start 2026-01-01 --end 2026-02-01
    marketpulse fx-sync --since 2026-01-01
    marketpulse schemas check               # CI compatibility gate
    marketpulse topics create               # idempotent topic provisioning
"""

from __future__ import annotations

import asyncio
import socket
import sys
from datetime import datetime, timedelta
from typing import Annotated, Any

import typer

from marketpulse.config import AppSettings, get_settings
from marketpulse.contracts.models import BookTicker, FxRate, Kline, Trade
from marketpulse.contracts.registry import (
    SchemaRegistryClient,
    SchemaRegistryError,
    list_schema_files,
    load_schema,
    subject_for,
)
from marketpulse.logging import configure_logging, get_logger
from marketpulse.observability import IngestionMetrics, start_metrics_server
from marketpulse.utils.timeutil import utc_now

app = typer.Typer(
    name="marketpulse",
    help="Ingestion control plane for the MarketPulse lakehouse.",
    no_args_is_help=True,
    add_completion=False,
)
log = get_logger(__name__)

#: Logical stream -> contract model. One place to add a new stream.
STREAM_MODELS: dict[str, type] = {
    "trades": Trade,
    "book_ticker": BookTicker,
    "klines": Kline,
    "fx_rates": FxRate,
}


def _producer_id() -> str:
    """Stable-per-pod identity, so a duplicate storm is traceable to a replica."""
    return f"{socket.gethostname()}:{sys.argv[0].rsplit('/', 1)[-1]}"


def _bootstrap(settings: AppSettings) -> IngestionMetrics:
    configure_logging(
        level=settings.observability.log_level,
        json_logs=settings.observability.json_logs,
        service_name=settings.service_name,
    )
    metrics = IngestionMetrics()
    if start_metrics_server(settings.observability, metrics.registry):
        log.info("metrics.listening", port=settings.observability.metrics_port)
    return metrics


def _build_publisher(settings: AppSettings, metrics: IngestionMetrics, *, use_registry: bool) -> Any:
    from confluent_kafka import Producer  # noqa: PLC0415 - keeps CLI import light

    from marketpulse.ingestion.publisher import MarketDataPublisher  # noqa: PLC0415

    registry = (
        SchemaRegistryClient(settings.kafka.schema_registry_url) if use_registry else None
    )
    publisher = MarketDataPublisher(
        Producer(settings.kafka.producer_config()),
        settings.kafka,
        metrics,
        registry=registry,
        producer_id=_producer_id(),
    )
    publisher.bind_schemas({name: model for name, model in STREAM_MODELS.items()})
    return publisher


# ---------------------------------------------------------------------------
# Live feed
# ---------------------------------------------------------------------------
@app.command()
def stream(
    symbols: Annotated[
        str | None, typer.Option(help="Comma-separated override of the configured symbols.")
    ] = None,
    keep_raw: Annotated[
        bool, typer.Option(help="Persist the verbatim venue payload in raw_payload.")
    ] = True,
    use_registry: Annotated[
        bool, typer.Option(help="Register schemas with the Schema Registry on start-up.")
    ] = True,
) -> None:
    """Run the live websocket feed until SIGINT or SIGTERM."""
    settings = get_settings()
    if symbols:
        settings = settings.model_copy(
            update={
                "binance": settings.binance.model_copy(
                    update={"symbols": [s.strip().upper() for s in symbols.split(",") if s.strip()]}
                )
            }
        )
    metrics = _bootstrap(settings)

    from marketpulse.ingestion.binance_ws import (  # noqa: PLC0415
        BinanceMarketDataFeed,
        run_feed_until_signalled,
    )

    publisher = _build_publisher(settings, metrics, use_registry=use_registry)
    feed = BinanceMarketDataFeed(
        settings.binance,
        publisher,
        metrics,
        producer_id=_producer_id(),
        keep_raw_payload=keep_raw,
    )
    asyncio.run(run_feed_until_signalled(feed))


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------
@app.command()
def backfill(
    symbol: Annotated[str, typer.Option(help="Instrument symbol, e.g. BTCUSDT.")],
    start: Annotated[datetime, typer.Option(formats=["%Y-%m-%d"], help="Inclusive start date.")],
    end: Annotated[
        datetime | None,
        typer.Option(formats=["%Y-%m-%d"], help="Exclusive end date. Defaults to today."),
    ] = None,
    interval: Annotated[str, typer.Option(help="Candle interval, e.g. 1m.")] = "1m",
    use_registry: bool = True,
) -> None:
    """Backfill historical candles for one symbol over a half-open date range.

    Idempotent by construction: the window is derived from the arguments, never
    from now(), so re-running the same command produces the same rows and the
    silver dedup absorbs the overlap.
    """
    settings = get_settings()
    metrics = _bootstrap(settings)
    publisher = _build_publisher(settings, metrics, use_registry=use_registry)

    from datetime import timezone  # noqa: PLC0415

    window_start = start.replace(tzinfo=timezone.utc)
    window_end = (end or utc_now()).replace(tzinfo=timezone.utc)

    async def _run() -> int:
        from marketpulse.ingestion.binance_rest import (  # noqa: PLC0415
            BinanceRestClient,
            KlineWindow,
        )

        client = await BinanceRestClient.create(settings.binance, producer_id=_producer_id())
        published = 0
        try:
            window = KlineWindow(symbol.upper(), interval, window_start, window_end)
            skew = await client.server_time_skew_ms()
            log.info("backfill.clock_skew", skew_ms=skew)
            async for kline in client.iter_klines(window):
                if publisher.publish("klines", kline):
                    published += 1
        finally:
            await client.close()
            publisher.flush()
        return published

    count = asyncio.run(_run())
    typer.echo(
        f"backfilled {count} {interval} candles for {symbol.upper()} "
        f"[{window_start.date()} .. {window_end.date()})"
    )


@app.command("fx-sync")
def fx_sync(
    since: Annotated[
        datetime | None,
        typer.Option(formats=["%Y-%m-%d"], help="Earliest validity date to fetch."),
    ] = None,
    use_registry: bool = True,
) -> None:
    """Sync official USD/COP reference rates into the reference topic."""
    settings = get_settings()
    metrics = _bootstrap(settings)
    publisher = _build_publisher(settings, metrics, use_registry=use_registry)
    window_start = since or (utc_now() - timedelta(days=30))

    async def _run() -> int:
        from marketpulse.ingestion.fx_trm import TrmClient  # noqa: PLC0415

        client = await TrmClient.create(settings.fx, producer_id=_producer_id())
        try:
            rates = await client.fetch_since(window_start)
        finally:
            await client.close()
        published = sum(1 for rate in rates if publisher.publish("fx_rates", rate))
        publisher.flush()
        return published

    typer.echo(f"published {asyncio.run(_run())} FX rates since {window_start.date()}")


# ---------------------------------------------------------------------------
# Platform administration
# ---------------------------------------------------------------------------
schemas_app = typer.Typer(help="Schema Registry operations.", no_args_is_help=True)
app.add_typer(schemas_app, name="schemas")


@schemas_app.command("check")
def schemas_check() -> None:
    """Fail if any local schema is incompatible with what is registered.

    This is the CI gate from ADR-0006. It runs before merge, which is the only
    point at which an incompatible contract is cheap to fix.
    """
    settings = get_settings()
    configure_logging(level="INFO", json_logs=False)
    client = SchemaRegistryClient(settings.kafka.schema_registry_url)
    topics = settings.kafka.topics
    incompatible: list[str] = []

    for stream, model in STREAM_MODELS.items():
        subject = subject_for(topics[stream])
        schema = load_schema(model.avro_schema_file)
        if client.check_compatibility(subject, schema):
            typer.echo(f"  compatible    {subject}")
        else:
            incompatible.append(subject)
            typer.secho(f"  INCOMPATIBLE  {subject}", fg=typer.colors.RED)

    if incompatible:
        typer.secho(
            f"\n{len(incompatible)} subject(s) would break existing readers. "
            "See docs/adr/0006-schema-evolution-and-data-contracts.md",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)
    typer.secho(f"\nall {len(STREAM_MODELS)} subjects are BACKWARD compatible", fg=typer.colors.GREEN)


@schemas_app.command("register")
def schemas_register(
    compatibility: Annotated[str, typer.Option(help="Level to pin on each subject.")] = "BACKWARD",
) -> None:
    """Register every local schema and pin its compatibility level."""
    settings = get_settings()
    configure_logging(level="INFO", json_logs=False)
    client = SchemaRegistryClient(settings.kafka.schema_registry_url)
    topics = settings.kafka.topics

    for stream, model in STREAM_MODELS.items():
        subject = subject_for(topics[stream])
        try:
            schema_id = client.register(subject, load_schema(model.avro_schema_file))
            client.set_compatibility(subject, compatibility)
            typer.echo(f"  registered {subject} -> id {schema_id} ({compatibility})")
        except SchemaRegistryError as exc:
            typer.secho(f"  failed {subject}: {exc}", fg=typer.colors.RED)
            raise typer.Exit(code=1) from exc

    typer.echo(f"\n{len(list_schema_files())} schema files on disk, {len(STREAM_MODELS)} registered")


@app.command("topics")
def topics(
    action: Annotated[str, typer.Argument(help="Only 'create' is supported.")] = "create",
    partitions: Annotated[int, typer.Option(help="Partitions per market-data topic.")] = 6,
    replication: Annotated[int, typer.Option(help="Replication factor.")] = 1,
    retention_hours: Annotated[int, typer.Option(help="Retention for market-data topics.")] = 168,
) -> None:
    """Provision topics idempotently.

    Auto-creation is disabled on the broker on purpose: a typo in a topic name
    should be an error, not a new and permanently empty topic that someone
    finds three weeks later.
    """
    if action != "create":
        raise typer.BadParameter("only 'create' is supported")

    from confluent_kafka.admin import AdminClient, NewTopic  # noqa: PLC0415

    settings = get_settings()
    configure_logging(level="INFO", json_logs=False)
    admin = AdminClient({"bootstrap.servers": settings.kafka.bootstrap_servers})
    existing = set(admin.list_topics(timeout=10).topics)

    wanted = [
        NewTopic(
            name,
            num_partitions=partitions,
            replication_factor=replication,
            config={
                "retention.ms": str(retention_hours * 3_600_000),
                "compression.type": "producer",
                # Market data is an immutable log; compaction would be actively
                # wrong here, since every trade is a distinct fact.
                "cleanup.policy": "delete",
                "min.insync.replicas": str(min(replication, 2)),
            },
        )
        for name in settings.kafka.topics.values()
        if name not in existing
    ]

    if not wanted:
        typer.echo("all topics already exist; nothing to do")
        return

    for name, future in admin.create_topics(wanted).items():
        try:
            future.result()
            typer.echo(f"  created {name} ({partitions} partitions)")
        except Exception as exc:  # noqa: BLE001 - report per topic, keep going
            typer.secho(f"  failed  {name}: {exc}", fg=typer.colors.RED)


if __name__ == "__main__":  # pragma: no cover
    app()
