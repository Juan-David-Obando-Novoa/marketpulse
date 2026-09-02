"""SQL for the serving layer, kept out of the request handlers.

Two rules hold here.

**Every query is parameterised.** Not because these endpoints accept untrusted
input from the internet -- they may not -- but because a query built by string
formatting is one refactor away from being reachable from somewhere that does,
and by then nobody remembers which of forty query strings was safe.

**Every query has a bounded result.** An endpoint that can return the whole
minute fact table is an endpoint that will, eventually, from a retry loop, and
Trino will happily try. Limits are applied in SQL rather than trusted from the
caller.
"""

from __future__ import annotations

from dataclasses import dataclass

__all__ = ["MAX_PAGE_SIZE", "Query", "candles", "instruments", "liquidity_ranking", "quality_report"]

#: Hard ceiling regardless of what the caller asks for. 10,000 one-minute bars
#: is about a week of a single instrument, which is the largest window a chart
#: can meaningfully render.
MAX_PAGE_SIZE = 10_000


@dataclass(frozen=True, slots=True)
class Query:
    """A statement and its parameters, kept together so they cannot drift."""

    sql: str
    params: tuple[object, ...]


def instruments(*, tracked_only: bool) -> Query:
    return Query(
        sql="""
            select
                symbol,
                base_asset,
                quote_asset,
                instrument_class,
                tick_size,
                is_tracked,
                coverage_status,
                first_trade_seen_at,
                last_trade_seen_at,
                lifetime_trade_count
            from lakehouse.gold.dim_instrument
            where (? = false or is_tracked)
            order by lifetime_notional desc nulls last
        """,
        params=(tracked_only,),
    )


def candles(*, symbol: str, start: str, end: str, limit: int) -> Query:
    """One-minute bars for an instrument over a half-open window.

    The window is half-open in the API for the same reason it is half-open
    everywhere else in this codebase: two adjacent requests then tile exactly,
    and a client paging through a day does not get one duplicated bar per page
    boundary.
    """
    return Query(
        sql="""
            select
                symbol,
                bar_start,
                open_price,
                high_price,
                low_price,
                close_price,
                vwap,
                base_volume,
                quote_volume,
                trade_count,
                order_flow_imbalance,
                time_weighted_spread_bps,
                is_quote_gap
            from lakehouse.gold.fct_market_1m
            where symbol = ?
              and bar_start >= from_iso8601_timestamp(?)
              and bar_start <  from_iso8601_timestamp(?)
            order by bar_start
            limit ?
        """,
        params=(symbol, start, end, min(limit, MAX_PAGE_SIZE)),
    )


def liquidity_ranking(*, window_days: int, limit: int) -> Query:
    return Query(
        sql="""
            select
                window_days,
                symbol,
                quote_volume,
                mean_spread_bps,
                mean_touch_depth,
                realised_volatility,
                liquidity_score,
                liquidity_rank,
                turnover_rank,
                tightness_rank,
                days_observed
            from lakehouse.gold.mart_liquidity_ranking
            where window_days = ?
            order by liquidity_rank
            limit ?
        """,
        params=(window_days, min(limit, 200)),
    )


def quality_report(*, symbol: str | None, days: int) -> Query:
    """Per-instrument-day quality verdicts.

    Exposed through the API deliberately. A consumer who can see that
    2026-03-03 was flagged 'incomplete' will ask a different question than one
    who sees a number and assumes it is sound, and hiding the platform's own
    assessment of its data is how quiet corruption becomes accepted fact.
    """
    return Query(
        sql="""
            select
                symbol,
                trading_date,
                quality_verdict,
                minute_coverage_ratio,
                reconciliation_pass_ratio,
                duplicate_ratio,
                p99_ingestion_lag_ms,
                quote_gap_minutes,
                missed_book_updates
            from lakehouse.gold.mart_pipeline_health
            where (? is null or symbol = ?)
              and trading_date >= current_date - interval '1' day * ?
            order by trading_date desc, symbol
        """,
        params=(symbol, symbol, days),
    )
