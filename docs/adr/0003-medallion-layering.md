# 3. Bronze / Silver / Gold layering with hard contracts

- **Status:** Accepted
- **Date:** 2026-01-13

## Context

Without an explicit layering rule, transformation logic drifts into ingestion,
ingestion assumptions leak into marts, and a single upstream change breaks
everything at once. We also need a defensible answer to "can you reprocess the
last 30 days?" that does not involve re-hitting a rate-limited exchange API.

## Decision

Three layers, each with a rule that is enforced in review:

| Layer | Owner | Rule |
| --- | --- | --- |
| **Bronze** | Spark streaming / batch ingest | Append-only, immutable, *no business logic*. Source payload is preserved verbatim in `raw_payload` alongside typed columns. Ingestion metadata columns (`_ingested_at`, `_source`, `_kafka_offset`) are mandatory. |
| **Silver** | dbt (incremental) | Deduplicated, conformed, typed, timezone-normalised to UTC. One row per real-world event. Business entities, not source structures. |
| **Gold** | dbt (table / incremental) | Consumer-facing marts. Aggregations, metrics, and the dimensional model. Nothing downstream of gold may read silver directly. |

Bronze is the replay buffer of record: Kafka retention is days, bronze
retention is years. Any silver or gold table must be fully reconstructible from
bronze alone.

## Consequences

- Positive: reprocessing is a dbt full refresh, not an exchange backfill.
- Positive: schema drift in the source lands in bronze and is caught by tests
  at the bronze->silver boundary instead of corrupting marts.
- Negative: storage duplication (roughly 3x). At market-data volumes on object
  storage this is cheap relative to the operational cost of not having it.
- Negative: one more hop of latency. Silver is refreshed on a 5-minute cadence,
  which the consumers of this platform accept.
