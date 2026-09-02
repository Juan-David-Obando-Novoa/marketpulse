# Architecture Decision Records

Every non-obvious structural decision in this repository is written down as an
ADR before the code that implements it is merged. The format is Michael
Nygard's: **Context -> Decision -> Consequences**, with an explicit
*Alternatives considered* section, because the alternatives are usually the
interesting part six months later.

| ADR | Title | Status |
| --- | ----- | ------ |
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](0002-table-format-apache-iceberg.md) | Apache Iceberg as the table format | Accepted |
| [0003](0003-medallion-layering.md) | Bronze / Silver / Gold layering with hard contracts | Accepted |
| [0004](0004-streaming-engine-spark-structured-streaming.md) | Spark Structured Streaming for the Kafka to Iceberg hop | Accepted |
| [0005](0005-dagster-over-airflow.md) | Dagster over Airflow for orchestration | Accepted |
| [0006](0006-schema-evolution-and-data-contracts.md) | Data contracts and schema evolution policy | Accepted |
| [0007](0007-exactly-once-and-idempotency.md) | Delivery semantics and idempotency strategy | Accepted |
