# 7. Delivery semantics and idempotency strategy

- **Status:** Accepted
- **Date:** 2026-01-17

## Context

A websocket reconnect replays messages. A Spark restart reprocesses an
uncommitted micro-batch. A backfill overlaps an existing range. All three
produce duplicates, and "roughly the right trade count" is not an acceptable
answer for a market-data platform.

## Decision

**At-least-once everywhere, with idempotent keys, and deduplication pushed to
the silver layer.** Chasing exactly-once end-to-end across three systems buys
less than it costs.

- The producer runs with `enable.idempotence=true`, `acks=all`,
  `max.in.flight.requests.per.connection=5`, which removes duplicates
  *within* a producer session.
- Every record carries a deterministic **natural key**: for trades,
  `(exchange, symbol, trade_id)`; for BBO updates,
  `(exchange, symbol, update_id)`. The Kafka message key is `symbol`, so all
  events for an instrument land on one partition and stay ordered.
- Bronze is append-only and *tolerates* duplicates by design.
- Silver deduplicates with `row_number() over (partition by natural key order
  by _ingested_at desc) = 1` inside an incremental model with a lookback
  window wider than the maximum expected reconnect gap.

## Consequences

- Positive: every component can be restarted, replayed or backfilled without
  coordination. Reprocessing is safe by construction.
- Positive: duplicate rate is observable — bronze count minus silver count is a
  monitored metric, not a mystery.
- Negative: silver models pay a window function over the lookback window on
  every incremental run.
- Negative: consumers must read silver, never bronze. Enforced by ADR-0003 and
  by database-level grants in a production deployment.
