# 2. Apache Iceberg as the table format

- **Status:** Accepted
- **Date:** 2026-01-12

## Context

The lake stores append-heavy market data (millions of trades per day) on object
storage, and must be readable by several engines: Spark writes the streaming
ingest, Trino serves dbt and ad-hoc SQL, and PyIceberg drives maintenance.
Plain Parquet directories cannot give us atomic commits, schema evolution or
partition changes without a full rewrite, and they force every reader to do
expensive listing on object storage.

## Decision

Use **Apache Iceberg** as the table format for every layer of the lake, with a
**REST catalog** backed by PostgreSQL as the single catalog implementation, and
MinIO (S3-compatible) as storage.

Partitioning uses Iceberg's *hidden partitioning*:
`PARTITIONED BY (days(event_time), bucket(16, symbol))` for the high-volume
bronze tables. Consumers never write a partition predicate by hand.

## Alternatives considered

- **Delta Lake** — excellent Spark story, but the Trino connector and the
  non-Spark write path were weaker for our multi-engine requirement, and the
  protocol is more tightly coupled to one vendor's runtime.
- **Apache Hudi** — the strongest upsert/CDC story of the three, which we do
  not need: our workload is append-only with occasional MERGE for late data.
  Its operational surface (compaction services, table services) is larger.
- **Hive tables on Parquet** — rejected. No atomic commits, no safe schema
  evolution, and directory listing on object storage is a latency cliff.
- **Hive Metastore instead of a REST catalog** — rejected. It drags in Thrift
  and a JVM service whose failure modes are worse documented than the REST
  catalog's, and the REST spec lets us swap the implementation later (Polaris,
  Lakekeeper, Nessie, Glue) without touching engine configuration.

## Consequences

- Positive: atomic multi-file commits, time travel, `expire_snapshots` as a
  first-class retention mechanism, and partition evolution without rewriting.
- Positive: one catalog, four engines. Spark, Trino, dbt and PyIceberg all see
  the same table at the same snapshot.
- Negative: metadata files accumulate. Maintenance (compaction, snapshot
  expiry, orphan-file removal) becomes a first-class scheduled workload rather
  than an afterthought — see `orchestration/.../assets/maintenance.py`.
- Negative: small-file pressure from streaming writes is real. Mitigated with a
  60-second trigger interval and nightly `rewrite_data_files`.
