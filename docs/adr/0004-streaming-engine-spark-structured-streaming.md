# 4. Spark Structured Streaming for the Kafka to Iceberg hop

- **Status:** Accepted
- **Date:** 2026-01-14

## Context

Trades and best-bid/offer updates arrive on Kafka at a few thousand messages a
second. Something has to land them in Iceberg with at-least-once delivery,
bounded file sizes, and a recovery story that does not involve manual offset
surgery.

## Decision

Use **Spark Structured Streaming** with the Iceberg sink, one query per topic,
`trigger(processingTime="60 seconds")`, and checkpoints on S3/MinIO.

Offsets are owned by the Spark checkpoint, not by Kafka consumer groups, so
recovery is deterministic and replay is a matter of pointing at a new
checkpoint location with `startingOffsets`.

## Alternatives considered

- **Apache Flink** — genuinely better for low-latency, event-time-heavy
  processing with complex state. Rejected here because our bronze hop has *no*
  state beyond offsets: it parses, validates and appends. Paying Flink's
  operational cost for a stateless map is the wrong trade. Flink becomes the
  right answer the day we need sub-second windowed aggregation, and the Kafka
  topics are the seam that makes that swap possible.
- **Kafka Connect with the Iceberg sink** — the least code, and a reasonable
  choice. Rejected because dead-letter handling and payload validation would
  have to move into SMTs, which are harder to unit-test than PySpark code, and
  because we already need Spark in the stack for backfills.
- **A plain Python consumer writing with PyIceberg** — attractively simple, and
  used for the low-volume FX feed. Rejected for the high-volume topics: no
  built-in checkpointing, no backpressure, and small-file generation would be
  unmanageable.

## Consequences

- Positive: one engine for streaming ingest and batch backfill; the same code
  path with a different source.
- Positive: the 60-second trigger gives file sizes in the tens of megabytes
  rather than the kilobytes a per-record sink would produce.
- Negative: end-to-end latency of roughly one minute to bronze. Explicitly
  accepted; this is an analytics platform, not an execution system.
- Negative: Spark is the heaviest component in the local stack. It sits behind
  a compose profile so a laptop can run the rest without it.
