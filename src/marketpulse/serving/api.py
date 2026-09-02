"""The serving API.

A thin, typed, read-only HTTP layer over the gold marts. It exists so that a
consumer does not need Trino credentials, a JDBC driver, and knowledge of the
warehouse schema in order to answer three questions.

Design constraints worth stating, because they are what keep a serving layer
from quietly becoming a second transformation layer:

* **No business logic.** Every number returned is read from a gold table. If a
  field is missing, the answer is a dbt model, not a computed column here --
  otherwise the API and the warehouse start disagreeing and nobody can say
  which is right.
* **Read-only.** There is no write path, and the Trino user should have no
  write grants.
* **Bounded.** Every endpoint has a hard result ceiling enforced in SQL.
* **Honest about quality.** ``/v1/quality`` exposes the platform's own
  assessment of its data, and the candle response carries the quote-gap flag.
  Hiding that is how quiet corruption becomes accepted fact.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from marketpulse.__about__ import __version__
from marketpulse.logging import configure_logging, get_logger
from marketpulse.serving import queries

log = get_logger(__name__)
UTC = timezone.utc


# ---------------------------------------------------------------------------
# Connection handling
# ---------------------------------------------------------------------------
class TrinoGateway:
    """Synchronous Trino access, called off the event loop.

    The Trino DBAPI driver is blocking. Calling it directly from an async
    handler would stall every other in-flight request for the duration of the
    query, which on a warehouse query means seconds. ``run_in_threadpool`` is
    the correct adapter; pretending the driver is async is not.
    """

    def __init__(self, host: str, port: int, user: str, catalog: str) -> None:
        self._config = {"host": host, "port": port, "user": user, "catalog": catalog}

    def _connect(self) -> Any:
        import trino  # noqa: PLC0415

        return trino.dbapi.connect(**self._config, schema="gold")

    def _run(self, query: queries.Query) -> list[dict[str, Any]]:
        connection = self._connect()
        try:
            cursor = connection.cursor()
            cursor.execute(query.sql, query.params)
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
        finally:
            connection.close()

    async def fetch(self, query: queries.Query) -> list[dict[str, Any]]:
        return await run_in_threadpool(self._run, query)

    async def healthy(self) -> bool:
        try:
            await run_in_threadpool(self._run, queries.Query("select 1", ()))
        except Exception as exc:  # noqa: BLE001 - health must report, not raise
            log.warning("trino.health_check_failed", error=str(exc))
            return False
        return True


_gateway: TrinoGateway | None = None


def get_gateway() -> TrinoGateway:
    if _gateway is None:  # pragma: no cover - set during lifespan
        raise HTTPException(status_code=503, detail="serving layer is not ready")
    return _gateway


@asynccontextmanager
async def lifespan(app: FastAPI) -> Iterator[None]:
    # Set once here and read through a FastAPI dependency, which is the
    # framework's own idiom for process-wide state.
    global _gateway  # noqa: PLW0603
    configure_logging(level=os.getenv("MP_OBSERVABILITY__LOG_LEVEL", "INFO"))
    _gateway = TrinoGateway(
        host=os.getenv("MP_TRINO_HOST", "localhost"),
        port=int(os.getenv("MP_TRINO_PORT", "8090")),
        user=os.getenv("MP_TRINO_USER", "marketpulse-api"),
        catalog=os.getenv("MP_ICEBERG__CATALOG_NAME", "lakehouse"),
    )
    log.info("api.started", version=__version__)
    yield
    log.info("api.stopped")


app = FastAPI(
    title="MarketPulse",
    version=__version__,
    summary="Read-only analytics over the MarketPulse lakehouse.",
    description=__doc__,
    lifespan=lifespan,
)


@app.middleware("http")
async def add_timing_and_request_id(request: Request, call_next: Any) -> Response:
    """Attach a request id and a server-timing header to every response.

    The request id is echoed from the caller when present, so a trace spans the
    client and the platform rather than restarting at our edge.
    """
    request_id = request.headers.get("x-request-id") or f"mp-{int(time.time() * 1000)}"
    started = time.perf_counter()
    response: Response = await call_next(request)
    elapsed_ms = (time.perf_counter() - started) * 1000
    response.headers["x-request-id"] = request_id
    response.headers["server-timing"] = f"total;dur={elapsed_ms:.1f}"
    log.info(
        "http.request",
        request_id=request_id,
        method=request.method,
        path=request.url.path,
        status=response.status_code,
        duration_ms=round(elapsed_ms, 1),
    )
    return response


# ---------------------------------------------------------------------------
# Response models -- the API's own contract, separate from the warehouse schema
# ---------------------------------------------------------------------------
class Instrument(BaseModel):
    symbol: str
    base_asset: str | None = None
    quote_asset: str | None = None
    instrument_class: str | None = None
    tick_size: float | None = None
    is_tracked: bool = False
    coverage_status: str
    first_trade_seen_at: datetime | None = None
    last_trade_seen_at: datetime | None = None
    lifetime_trade_count: int = 0


class Candle(BaseModel):
    symbol: str
    bar_start: datetime
    open_price: float = Field(alias="open")
    high_price: float = Field(alias="high")
    low_price: float = Field(alias="low")
    close_price: float = Field(alias="close")
    vwap: float | None = None
    base_volume: float
    quote_volume: float
    trade_count: int
    order_flow_imbalance: float | None = None
    time_weighted_spread_bps: float | None = None
    is_quote_gap: bool = False

    model_config = {"populate_by_name": True}


class HealthResponse(BaseModel):
    status: str
    version: str
    warehouse_reachable: bool


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health", response_model=HealthResponse, tags=["operations"])
async def health(gateway: Annotated[TrinoGateway, Depends(get_gateway)]) -> HealthResponse:
    """Liveness and dependency readiness.

    Reports warehouse reachability rather than asserting it: a health endpoint
    that 500s when its dependency is down tells a load balancer to remove the
    only instance that could have served a cached answer.
    """
    reachable = await gateway.healthy()
    return HealthResponse(
        status="ok" if reachable else "degraded",
        version=__version__,
        warehouse_reachable=reachable,
    )


@app.get("/v1/instruments", response_model=list[Instrument], tags=["reference"])
async def list_instruments(
    gateway: Annotated[TrinoGateway, Depends(get_gateway)],
    tracked_only: Annotated[
        bool, Query(description="Only instruments the platform tracks.")
    ] = True,
) -> list[dict[str, Any]]:
    return await gateway.fetch(queries.instruments(tracked_only=tracked_only))


@app.get("/v1/candles/{symbol}", response_model=list[Candle], tags=["market data"])
async def get_candles(  # noqa: PLR0917 - FastAPI declares its interface as parameters
    symbol: str,
    gateway: Annotated[TrinoGateway, Depends(get_gateway)],
    start: Annotated[datetime | None, Query(description="Inclusive, ISO 8601.")] = None,
    end: Annotated[datetime | None, Query(description="Exclusive, ISO 8601.")] = None,
    limit: Annotated[int, Query(ge=1, le=queries.MAX_PAGE_SIZE)] = 1_000,
    response: Response = None,  # type: ignore[assignment]
) -> list[dict[str, Any]]:
    """One-minute bars over a half-open window.

    Half-open so that two adjacent requests tile exactly and a client paging
    through a day never receives a duplicated bar at a page boundary.
    """
    window_end = end or datetime.now(tz=UTC)
    window_start = start or (window_end - timedelta(hours=6))
    if window_end <= window_start:
        raise HTTPException(status_code=422, detail="end must be after start")
    if window_end - window_start > timedelta(days=31):
        raise HTTPException(
            status_code=422,
            detail="window exceeds 31 days; page the request rather than widening it",
        )

    rows = await gateway.fetch(
        queries.candles(
            symbol=symbol.upper(),
            start=window_start.isoformat(),
            end=window_end.isoformat(),
            limit=limit,
        )
    )
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"no bars for {symbol.upper()} in the requested window",
        )
    if response is not None and len(rows) == min(limit, queries.MAX_PAGE_SIZE):
        # The caller cannot otherwise tell a complete window from a truncated
        # one, and silently truncated results are how a chart ends early
        # without anyone noticing.
        response.headers["x-result-truncated"] = "true"
    return rows


@app.get("/v1/liquidity", tags=["analytics"])
async def liquidity_ranking(
    gateway: Annotated[TrinoGateway, Depends(get_gateway)],
    window_days: Annotated[int, Query(description="1, 7 or 30.")] = 7,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> list[dict[str, Any]]:
    """Instruments ranked by turnover per basis point of spread.

    Only instrument-days the platform assessed as good are included, which is
    enforced upstream in the mart: ranking on data known to be incomplete
    produces a confidently wrong answer.
    """
    if window_days not in (1, 7, 30):
        raise HTTPException(status_code=422, detail="window_days must be 1, 7 or 30")
    return await gateway.fetch(queries.liquidity_ranking(window_days=window_days, limit=limit))


@app.get("/v1/quality", tags=["operations"])
async def quality(
    gateway: Annotated[TrinoGateway, Depends(get_gateway)],
    symbol: Annotated[str | None, Query()] = None,
    days: Annotated[int, Query(ge=1, le=90)] = 7,
) -> list[dict[str, Any]]:
    """The platform's own assessment of its data, per instrument per day.

    Deliberately public. A consumer who can see that a date was flagged
    'incomplete' asks a different question than one who sees a number and
    assumes it is sound.
    """
    return await gateway.fetch(
        queries.quality_report(symbol=symbol.upper() if symbol else None, days=days)
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Never leak a stack trace or a SQL fragment to a caller."""
    log.error("http.unhandled", path=request.url.path, error=str(exc), exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "internal error", "request_id": request.headers.get("x-request-id")},
    )
