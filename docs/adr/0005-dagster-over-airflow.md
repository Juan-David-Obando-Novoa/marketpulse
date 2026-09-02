# 5. Dagster over Airflow for orchestration

- **Status:** Accepted
- **Date:** 2026-01-15

## Context

The platform needs scheduling, backfills over date partitions, dependency
resolution across roughly forty dbt models plus ingestion and maintenance
steps, and a way to express "this table is stale" rather than "this task
failed".

## Decision

Use **Dagster** with the software-defined-asset model. Each Iceberg table and
each dbt model is an asset; `dagster-dbt` loads the dbt manifest so that dbt's
DAG and the platform's DAG are one graph rather than two.

Data quality lives in **asset checks** attached to the asset they guard, so a
freshness or volume violation is visible on the same object a consumer looks at
when they ask "is this table good?".

## Alternatives considered

- **Apache Airflow** — the industry default, the largest ecosystem, and the
  option a hiring manager is most likely to already run. Rejected because it
  orchestrates *tasks*, not *data*: partition-aware backfill of an asset graph
  requires bespoke code, and dbt integration is a `BashOperator` unless you
  adopt Cosmos. The trade is deliberate and reversible — the ingestion and dbt
  layers are plain CLIs, so an Airflow DAG could drive them tomorrow.
- **Prefect** — good ergonomics, weaker asset/lineage model for a warehouse-
  centric workload.
- **dbt Cloud / cron** — no coverage for the non-dbt half of the platform.

## Consequences

- Positive: backfilling 2026-01-01..2026-02-01 is a partition range selection in
  the UI, not a hand-written script.
- Positive: lineage from Kafka topic to gold mart is one graph, and OpenLineage
  events are emitted from it.
- Negative: smaller community than Airflow; some integrations must be written
  by hand.
