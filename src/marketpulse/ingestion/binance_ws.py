"""Binance public websocket feed.

The interesting part of a market-data client is not the happy path -- reading
JSON off a socket is ten lines. It is everything around it:

* **A socket that is open but silent is the common failure**, not a socket that
  closes. TCP keepalive will not notice, and neither will a naive
  ``async for message in ws``. An idle watchdog is what turns that into a
  reconnect instead of a four-hour gap nobody spots until the next morning.
* **Reconnects must be jittered**, or every replica returns in lockstep and the
  venue's rate limiter treats the fleet as an attack (ADR: see utils/backoff).
* **A malformed message must not kill the loop.** It goes to the dead-letter
  topic and the loop continues (ADR-0006).
* **Shutdown must be graceful.** SIGTERM drains the producer queue rather than
  dropping whatever librdkafka is still holding.

The connection is created through an injected factory so the whole state
machine can be driven by a scripted fake socket in the unit tests.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TYPE_CHECKING, Any

from marketpulse.contracts.models import BookTicker, Trade
from marketpulse.ingestion.normalizers import (
    EXCHANGE,
    NormalizationError,
    SequenceTracker,
    normalise_book_ticker,
    normalise_trade,
    split_combined_stream,
    stream_kind,
)
from marketpulse.logging import get_logger
from marketpulse.utils.backoff import ExponentialBackoff
from marketpulse.utils.timeutil import utc_now

if TYPE_CHECKING:
    from marketpulse.config import BinanceSettings
    from marketpulse.ingestion.publisher import MarketDataPublisher
    from marketpulse.observability import IngestionMetrics

__all__ = ["BinanceMarketDataFeed", "SocketLike", "build_stream_url", "websocket_factory"]

log = get_logger(__name__)

#: Binance caps a single combined-stream subscription. Beyond this the client
#: must shard across connections; we fail loudly rather than silently dropping
#: the tail of the subscription list, which is what the venue does.
MAX_STREAMS_PER_CONNECTION = 200


class SocketLike:
    """Structural type for the subset of a websocket we use.

    Not a Protocol because ``async for`` support is what matters and expressing
    that structurally adds noise without adding safety here.
    """

    async def __aiter__(self) -> AsyncIterator[str]: ...  # pragma: no cover

    async def close(self) -> None: ...  # pragma: no cover


def build_stream_url(base_url: str, stream_names: list[str]) -> str:
    """Build a combined-stream URL, refusing an oversized subscription.

    Binance silently truncates a subscription that exceeds its limit, which
    presents downstream as "one symbol just has no data" -- among the more
    annoying bugs to diagnose. Failing here makes it a start-up error.
    """
    if not stream_names:
        raise ValueError("at least one stream must be subscribed")
    if len(stream_names) > MAX_STREAMS_PER_CONNECTION:
        raise ValueError(
            f"{len(stream_names)} streams exceeds the {MAX_STREAMS_PER_CONNECTION} "
            "per-connection limit; shard across connections instead of letting "
            "the venue silently truncate the subscription"
        )
    return f"{base_url}?streams={'/'.join(stream_names)}"


def websocket_factory(settings: BinanceSettings) -> Callable[[str], Any]:
    """Return a callable that opens a websocket with venue-appropriate options.

    Imported lazily so that unit tests -- which inject a fake -- do not need
    the ``websockets`` package installed at all.
    """

    def _connect(url: str) -> Any:
        import websockets  # noqa: PLC0415 - deliberately lazy

        return websockets.connect(
            url,
            ping_interval=settings.ping_interval_seconds,
            ping_timeout=settings.ping_interval_seconds,
            close_timeout=5,
            max_queue=4_096,
        )

    return _connect


class BinanceMarketDataFeed:
    """Consume the venue's combined stream and publish contract records.

    One instance owns one websocket connection and the reconnect state machine
    around it.
    """

    def __init__(
        self,
        settings: BinanceSettings,
        publisher: MarketDataPublisher,
        metrics: IngestionMetrics,
        *,
        producer_id: str,
        connect: Callable[[str], Any] | None = None,
        keep_raw_payload: bool = True,
    ) -> None:
        self._settings = settings
        self._publisher = publisher
        self._metrics = metrics
        self._producer_id = producer_id
        self._connect = connect or websocket_factory(settings)
        self._keep_raw = keep_raw_payload
        self._sequences = SequenceTracker()
        self._backoff = ExponentialBackoff(
            initial=settings.reconnect_initial_backoff_seconds,
            maximum=settings.reconnect_max_backoff_seconds,
        )
        self._stop = asyncio.Event()
        self._messages_seen = 0

    @property
    def messages_seen(self) -> int:
        """Total messages accepted from the socket since construction."""
        return self._messages_seen

    def request_stop(self) -> None:
        """Ask the run loop to exit after the current message. Signal-safe."""
        self._stop.set()

    async def run(self) -> None:
        """Connect, consume and reconnect until :meth:`request_stop` is called."""
        url = build_stream_url(self._settings.ws_base_url, self._settings.stream_names)
        log.info("feed.starting", url=url, streams=len(self._settings.stream_names))

        while not self._stop.is_set():
            try:
                await self._run_once(url)
                reason = "clean_close"
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - any failure is a reconnect
                reason = type(exc).__name__
                log.warning("feed.connection_lost", error=str(exc), reason=reason)
            finally:
                self._metrics.feed_connected.labels(source=EXCHANGE).set(0)

            if self._stop.is_set():
                break

            # A reconnect replays and re-seeds the venue sequence, so any gap
            # measured across the boundary is an artefact, not data loss.
            self._sequences.reset()
            self._metrics.reconnects.labels(source=EXCHANGE, reason=reason).inc()
            delay = self._backoff.next_delay()
            log.info("feed.reconnecting", delay_seconds=round(delay, 2), attempt=self._backoff.attempt)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=delay)

        log.info("feed.stopped", messages_seen=self._messages_seen)

    async def _run_once(self, url: str) -> None:
        """One connection lifetime: connect, consume until idle or closed."""
        async with self._connect(url) as socket:
            self._backoff.reset()
            self._metrics.feed_connected.labels(source=EXCHANGE).set(1)
            log.info("feed.connected")

            iterator = socket.__aiter__()
            while not self._stop.is_set():
                try:
                    raw = await asyncio.wait_for(
                        iterator.__anext__(),
                        timeout=self._settings.idle_timeout_seconds,
                    )
                except StopAsyncIteration:
                    return
                except asyncio.TimeoutError:
                    # The socket is open but the venue has gone quiet. This is
                    # the failure mode TCP will not surface for us.
                    raise ConnectionError(
                        f"no message for {self._settings.idle_timeout_seconds}s; "
                        "treating the socket as dead"
                    ) from None
                self._handle_raw(raw)
            await socket.close()

    def _handle_raw(self, raw: str | bytes) -> None:
        """Parse, normalise and publish one websocket frame.

        Every failure below is per-message: it is counted, dead-lettered and
        stepped over. Nothing here may raise, because raising ends the
        connection and drops the other nineteen symbols with it.
        """
        self._messages_seen += 1
        text = raw.decode() if isinstance(raw, bytes) else raw
        received_at = utc_now()

        try:
            envelope = json.loads(text)
        except json.JSONDecodeError as exc:
            self._publisher.publish_dead_letter(
                exc, origin_topic="unknown", raw_payload=text[:64_000]
            )
            return

        if not isinstance(envelope, dict):
            return
        if "result" in envelope or "error" in envelope:
            # Subscription control frames, not market data.
            log.debug("feed.control_frame", frame=envelope)
            return

        stream_name, payload = split_combined_stream(envelope)
        kind = stream_kind(stream_name, payload)
        self._metrics.messages_received.labels(source=EXCHANGE, stream=kind).inc()
        self._metrics.last_message_timestamp.labels(source=EXCHANGE, stream=kind).set(
            received_at.timestamp()
        )

        try:
            if kind == "trade":
                self._publish_trade(payload, received_at, kind)
            elif kind == "bookTicker":
                self._publish_book_ticker(payload, received_at, kind)
            else:
                log.debug("feed.unhandled_stream", stream=stream_name, kind=kind)
        except (NormalizationError, ValueError) as exc:
            topic = "md.trades.v1" if kind == "trade" else "md.book_ticker.v1"
            self._publisher.publish_dead_letter(
                exc, origin_topic=topic, raw_payload=text[:64_000], origin_stream=stream_name
            )

    def _publish_trade(self, payload: dict[str, Any], received_at: Any, kind: str) -> None:
        trade: Trade = normalise_trade(
            payload,
            producer_id=self._producer_id,
            received_at=received_at,
            keep_raw=self._keep_raw,
        )
        self._metrics.observe_lag(EXCHANGE, kind, trade.lag_millis())
        self._publisher.publish("trades", trade)

    def _publish_book_ticker(self, payload: dict[str, Any], received_at: Any, kind: str) -> None:
        ticker: BookTicker = normalise_book_ticker(
            payload,
            producer_id=self._producer_id,
            received_at=received_at,
            keep_raw=self._keep_raw,
        )
        missed = self._sequences.observe(ticker.symbol, ticker.update_id)
        if missed:
            # The venue's update_id is monotonic per symbol; a jump means the
            # venue sent updates we never saw. Silent loss, made countable.
            self._metrics.sequence_gaps.labels(source=EXCHANGE, symbol=ticker.symbol).inc(missed)
            log.warning("feed.sequence_gap", symbol=ticker.symbol, missed=missed)
        self._metrics.observe_lag(EXCHANGE, kind, ticker.lag_millis())
        self._publisher.publish("book_ticker", ticker)


async def run_feed_until_signalled(
    feed: BinanceMarketDataFeed,
    *,
    shutdown_grace_seconds: float = 30.0,
    install_signal_handlers: bool = True,
) -> None:
    """Run ``feed`` until SIGINT/SIGTERM, then drain the producer queue.

    Draining on shutdown is the difference between a clean rolling restart and
    losing whatever librdkafka happened to be buffering.
    """
    import signal  # noqa: PLC0415 - only needed on the process entry path

    loop = asyncio.get_running_loop()
    if install_signal_handlers:
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, feed.request_stop)

    task: asyncio.Task[None] = asyncio.create_task(feed.run())
    try:
        await task
    finally:
        await asyncio.to_thread(feed._publisher.flush, shutdown_grace_seconds)  # noqa: SLF001
