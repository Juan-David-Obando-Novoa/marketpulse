"""Binance REST client for historical backfill.

The websocket gives us *now*; this gives us *before*. Two jobs need it:
seeding a new symbol with history, and reconciling our trade-derived candles
against the venue's own -- an independent check that catches an entire class of
aggregation bug that comparing our data to itself never will.

Three concerns dominate a backfill client against a public API:

* **Rate limits are a shared budget, not a per-request property.** The venue
  publishes a weight budget per minute and returns ``X-MBX-USED-WEIGHT-1M`` on
  every response. This client reads that header and paces itself against the
  venue's own accounting rather than guessing from request counts.
* **429 and 418 are different.** 429 means slow down; 418 means you were told
  to slow down and did not, and you are now banned for a while. ``Retry-After``
  is honoured on both, and a 418 is escalated rather than retried blindly.
* **Windows must be deterministic.** Backfill is re-run constantly (a failed
  partition, a widened date range), so the same request must produce the same
  rows. Windows are closed-open and derived from the partition, never from
  ``now()``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from marketpulse.contracts.models import Kline
from marketpulse.ingestion.normalizers import normalise_rest_kline
from marketpulse.logging import get_logger
from marketpulse.utils.backoff import ExponentialBackoff
from marketpulse.utils.timeutil import datetime_to_epoch_millis, utc_now

if TYPE_CHECKING:
    from marketpulse.config import BinanceSettings

__all__ = ["BinanceRestClient", "KlineWindow", "RateLimitError", "interval_to_timedelta"]

log = get_logger(__name__)

#: The venue caps a single klines response. Requesting more is not an error --
#: it silently returns this many, which turns a naive backfill into a gap.
MAX_KLINES_PER_REQUEST = 1_000

#: Weight budget headroom. We back off once the venue reports we have consumed
#: this fraction of the minute's allowance, rather than waiting for the 429.
WEIGHT_SOFT_LIMIT_RATIO = 0.75

_INTERVALS: dict[str, timedelta] = {
    "1m": timedelta(minutes=1),
    "3m": timedelta(minutes=3),
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "30m": timedelta(minutes=30),
    "1h": timedelta(hours=1),
    "2h": timedelta(hours=2),
    "4h": timedelta(hours=4),
    "6h": timedelta(hours=6),
    "8h": timedelta(hours=8),
    "12h": timedelta(hours=12),
    "1d": timedelta(days=1),
    "3d": timedelta(days=3),
    "1w": timedelta(weeks=1),
}


class RateLimitError(RuntimeError):
    """The venue asked us to stop. Carries the wait it demanded."""

    def __init__(self, message: str, retry_after_seconds: float, banned: bool = False) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds
        self.banned = banned


def interval_to_timedelta(interval: str) -> timedelta:
    """Translate a venue interval code into a duration."""
    try:
        return _INTERVALS[interval]
    except KeyError as exc:
        raise ValueError(
            f"unsupported interval {interval!r}; known: {sorted(_INTERVALS)}"
        ) from exc


@dataclass(frozen=True, slots=True)
class KlineWindow:
    """A closed-open request window: ``[start, end)``.

    Closed-open is not pedantry. Half-open windows tile without overlap, which
    is what makes a re-run of one partition idempotent and a re-run of two
    adjacent partitions free of duplicated candles at the seam.
    """

    symbol: str
    interval: str
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.end <= self.start:
            raise ValueError(f"window end {self.end} must be after start {self.start}")

    @property
    def expected_candles(self) -> int:
        return int((self.end - self.start) / interval_to_timedelta(self.interval))

    def chunks(self, limit: int = MAX_KLINES_PER_REQUEST) -> list[KlineWindow]:
        """Split into sub-windows that each fit in one response.

        This is the whole reason the venue's silent truncation does not become
        a data gap: the caller never asks for more than one page.
        """
        step = interval_to_timedelta(self.interval) * limit
        windows: list[KlineWindow] = []
        cursor = self.start
        while cursor < self.end:
            windows.append(
                KlineWindow(self.symbol, self.interval, cursor, min(cursor + step, self.end))
            )
            cursor += step
        return windows


class BinanceRestClient:
    """Async REST client with venue-aware pacing.

    The HTTP session is injected so tests can supply a stub; in production it
    is an ``aiohttp.ClientSession`` created by :meth:`create`.
    """

    def __init__(
        self,
        settings: BinanceSettings,
        session: Any,
        *,
        producer_id: str = "backfill",
    ) -> None:
        self._settings = settings
        self._session = session
        self._producer_id = producer_id
        self._used_weight = 0
        self._backoff = ExponentialBackoff(initial=1.0, maximum=60.0)

    @classmethod
    async def create(
        cls, settings: BinanceSettings, *, producer_id: str = "backfill"
    ) -> BinanceRestClient:
        """Open an owned aiohttp session. Caller is responsible for :meth:`close`."""
        import aiohttp  # noqa: PLC0415 - lazy so unit tests need no aiohttp

        session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={"User-Agent": "marketpulse/0.1 (+https://github.com/juandavidobando)"},
        )
        return cls(settings, session, producer_id=producer_id)

    async def close(self) -> None:
        close = getattr(self._session, "close", None)
        if close is not None:
            await close()

    @property
    def used_weight(self) -> int:
        """Venue-reported request weight consumed in the current minute."""
        return self._used_weight

    async def _get(self, path: str, params: dict[str, Any]) -> Any:
        """One GET with rate-limit accounting and bounded retries."""
        url = f"{self._settings.rest_base_url}{path}"
        attempts = 0
        while True:
            attempts += 1
            async with self._session.get(url, params=params) as response:
                self._absorb_rate_limit_headers(response.headers)
                if response.status == 200:
                    self._backoff.reset()
                    await self._respect_weight_budget()
                    return await response.json()

                retry_after = float(response.headers.get("Retry-After", 0) or 0)
                if response.status in (429, 418):
                    banned = response.status == 418
                    wait = retry_after or self._backoff.next_delay()
                    log.warning(
                        "rest.rate_limited",
                        status=response.status,
                        wait_seconds=round(wait, 2),
                        banned=banned,
                        used_weight=self._used_weight,
                    )
                    if banned:
                        # An IP ban is not something to retry our way out of.
                        raise RateLimitError(
                            f"venue returned 418 (IP ban) for {url}", wait, banned=True
                        )
                    await asyncio.sleep(wait)
                    continue

                if 500 <= response.status < 600 and attempts <= 5:
                    wait = self._backoff.next_delay()
                    log.warning("rest.server_error", status=response.status, wait_seconds=wait)
                    await asyncio.sleep(wait)
                    continue

                body = await response.text()
                raise RuntimeError(f"GET {url} failed with {response.status}: {body[:500]}")

    def _absorb_rate_limit_headers(self, headers: Any) -> None:
        """Track the venue's own view of our consumption.

        Counting requests locally is guesswork; different endpoints carry
        different weights and the venue is the only authority on the total.
        """
        for key in ("X-MBX-USED-WEIGHT-1M", "x-mbx-used-weight-1m"):
            raw = headers.get(key) if hasattr(headers, "get") else None
            if raw is not None:
                try:
                    self._used_weight = int(raw)
                except (TypeError, ValueError):  # pragma: no cover - venue always sends an int
                    pass
                return

    async def _respect_weight_budget(self) -> None:
        """Pause before the venue has to say no.

        Backing off at 75% of the published budget keeps the backfill inside
        the limit even when a streaming job shares the same source IP.
        """
        soft_limit = self._settings.rest_max_requests_per_minute * WEIGHT_SOFT_LIMIT_RATIO
        if self._used_weight >= soft_limit:
            log.info("rest.self_throttling", used_weight=self._used_weight, soft_limit=soft_limit)
            await asyncio.sleep(2.0)

    async def fetch_klines(self, window: KlineWindow) -> list[Kline]:
        """Fetch every candle in ``window``, paging as needed.

        The venue's ``startTime``/``endTime`` are inclusive on both ends, so
        the exclusive end of our half-open window is converted by subtracting a
        millisecond. Without that, adjacent partitions duplicate one candle at
        every seam -- a bug that survives a long time because the row counts
        look almost right.
        """
        collected: list[Kline] = []
        received_at = utc_now()
        for chunk in window.chunks():
            rows = await self._get(
                "/api/v3/klines",
                {
                    "symbol": chunk.symbol,
                    "interval": chunk.interval,
                    "startTime": datetime_to_epoch_millis(chunk.start),
                    "endTime": datetime_to_epoch_millis(chunk.end) - 1,
                    "limit": MAX_KLINES_PER_REQUEST,
                },
            )
            collected.extend(
                normalise_rest_kline(
                    row,
                    symbol=chunk.symbol,
                    interval=chunk.interval,
                    producer_id=self._producer_id,
                    received_at=received_at,
                )
                for row in rows
            )

        expected = window.expected_candles
        if collected and len(collected) < expected:
            # Not fatal: venues have genuine gaps (maintenance windows, halted
            # instruments). It is recorded so the completeness check downstream
            # has something to compare against.
            log.warning(
                "backfill.incomplete_window",
                symbol=window.symbol,
                interval=window.interval,
                expected=expected,
                received=len(collected),
                start=window.start.isoformat(),
            )
        return collected

    async def iter_klines(self, window: KlineWindow) -> AsyncIterator[Kline]:
        """Stream candles one at a time, so a long backfill is not held in memory."""
        for chunk in window.chunks():
            for kline in await self.fetch_klines(chunk):
                yield kline

    async def exchange_info(self, symbols: list[str] | None = None) -> dict[str, Any]:
        """Instrument reference data: tick size, lot size, status.

        This is the source for the SCD2 instrument dimension. Filters change
        without notice, and a trade that violates the *current* tick size is
        usually a stale dimension rather than bad data.
        """
        params: dict[str, Any] = {}
        if symbols:
            params["symbols"] = "[" + ",".join(f'"{s}"' for s in symbols) + "]"
        return await self._get("/api/v3/exchangeInfo", params)  # type: ignore[no-any-return]

    async def server_time_skew_ms(self) -> int:
        """Local clock minus venue clock, in milliseconds.

        Worth measuring: every latency metric in this platform is computed
        against venue timestamps, so an unnoticed clock skew silently biases
        the whole lag histogram.
        """
        payload = await self._get("/api/v3/time", {})
        return datetime_to_epoch_millis(utc_now()) - int(payload["serverTime"])
