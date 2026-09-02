# 6. Data contracts and schema evolution policy

- **Status:** Accepted
- **Date:** 2026-01-16

## Context

The upstream is a third-party exchange we do not control. Fields appear,
optional fields turn out to be required, and a silent type change from string
to number is a Tuesday. Meanwhile downstream consumers need a stable promise.

## Decision

1. **The contract is the Avro schema**, versioned in `src/marketpulse/contracts/schemas/`
   and registered in the Redpanda Schema Registry with **`BACKWARD`**
   compatibility. Producers cannot publish a schema that breaks existing
   readers; CI enforces this by running a compatibility check against the
   registry before merge.
2. **Pydantic models mirror the Avro schemas** and are the validation boundary
   at the edge of the producer. A payload that fails validation is not dropped
   and is not allowed to crash the producer: it goes to the topic's
   dead-letter twin with the validation error attached.
3. **Additive-only evolution.** New fields must have defaults. Removing or
   retyping a field requires a new topic version (`md.trades.v2`) and a
   documented dual-write window.
4. **Iceberg schema evolution** is used for widening in bronze; column renames
   go through Iceberg's ID-based rename, never a drop-and-add.

## Consequences

- Positive: a malformed upstream payload degrades one message, not the pipeline.
- Positive: the dead-letter topic is a queryable record of upstream drift and
  feeds a Grafana panel rather than a pager.
- Negative: dual-write windows are operational work. Accepted as the price of
  never breaking a consumer.
