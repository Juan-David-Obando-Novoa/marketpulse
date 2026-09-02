# Running the platform

`make` is the intended interface and every target here has one. This document
spells out the underlying commands as well, because `make` is not present on a
default Windows install and because knowing what a target actually does is the
difference between operating a stack and hoping at it.

Commands are shown in PowerShell. On macOS or Linux they are identical minus
the `Copy-Item`, and `make <target>` is shorter.

---

## A note on profiles

Compose profiles here are **cumulative**. Every tier depends on the core one,
and Compose refuses a project whose `depends_on` names a service that no active
profile contains -- so `--profile ingestion` on its own fails with
*"service producer depends on undefined service redpanda"*. Each command below
therefore passes core alongside whatever tier it needs. `make` hides this; the
raw commands cannot.

## Before you start

| Requirement | Check | Notes |
|---|---|---|
| Docker Desktop, running | `docker compose version` | WSL2 backend on Windows |
| ~10 GB of memory for Docker | Docker Desktop → Settings → Resources | Spark and Trino are the hungry ones |
| ~15 GB of free disk | | Images total roughly 8 GB; the Spark image alone pulls ~250 MB of jars |
| Outbound internet | | For the venue feed and for pulling images |

No API keys are needed at any point.

---

## 1. Configuration

```powershell
Copy-Item .env.example .env
```

Every value in it is a local development credential. Nothing needs editing for
a first run.

*Make equivalent: this happens automatically as a prerequisite of any target.*

---

## 2. The core tier

Broker, object storage, catalog database and the Iceberg REST catalog.

```powershell
docker compose --profile core up -d
docker compose --profile core ps
```

Wait until every service reports `healthy`. Roughly 45 seconds on a warm
machine, longer on the first run while images pull.

**Checkpoint** — all four should answer:

```powershell
docker compose exec -T redpanda rpk cluster health
curl.exe -s "http://localhost:8181/v1/config?warehouse=s3://lakehouse/warehouse"
Start-Process http://localhost:9001   # MinIO console, minioadmin / minioadmin
Start-Process http://localhost:8080   # Redpanda console
```

*Make equivalent:* `make up`

---

## 3. Bootstrap: topics, schemas and tables

The producer image carries the CLI, so provisioning runs through it. The first
invocation builds the image.

```powershell
docker compose exec -T redpanda rpk cluster config set auto_create_topics_enabled false
docker compose --profile core --profile ingestion build producer
docker compose --profile core --profile ingestion run --rm producer marketpulse topics create
docker compose --profile core --profile ingestion run --rm producer marketpulse schemas register
```

The first line disables broker-side topic auto-creation. It is a cluster
property rather than a node one, so it is set through the admin API after the
broker is up rather than as a start flag -- and it is run from inside the
container, where rpk finds the admin API on localhost without being told.

`topics create` provisions five topics with explicit retention and partitioning;
broker-side auto-creation is deliberately disabled, so this step is required
rather than optional. `schemas register` publishes the five Avro contracts and
pins each subject to `BACKWARD` compatibility.

**Checkpoint:**

```powershell
docker compose exec -T redpanda rpk topic list
curl.exe -s http://localhost:18081/subjects
```

You should see `md.trades.v1`, `md.book_ticker.v1`, `md.klines.v1`,
`ref.fx_rates.v1`, `md.dead_letter.v1` and five registered subjects.

---

## 4. The processing tier

Spark and Trino. **This is the slow step** — the Spark image downloads the
Iceberg, Kafka and S3 jars at build time, which is deliberate (a streaming job
that resolves dependencies on restart cannot start during a network incident)
but does mean five to ten minutes the first time.

```powershell
docker compose --profile core --profile processing up -d --build
docker compose --profile core --profile processing ps
```

Then create the Iceberg tables. They are created by explicit DDL rather than
inferred by the first write, so that the partition specs are the intended ones:

```powershell
docker compose exec -T spark spark-sql -f /opt/marketpulse/src/marketpulse/streaming/ddl/bronze.sql
```

**Checkpoint:**

```powershell
docker compose exec -T trino trino --execute "show schemas from lakehouse"
docker compose exec -T trino trino --execute "show tables from lakehouse.bronze"
```

*Make equivalent:* `make bootstrap`

---

## 5. Start ingesting

```powershell
docker compose --profile core --profile ingestion up -d producer
docker compose logs -f producer
```

Within a couple of seconds you should see `feed.connected` and then a steady
stream of publishes. `Ctrl+C` stops tailing, not the container.

Now start the two streaming jobs that land Kafka into Iceberg:

```powershell
docker compose exec -d spark spark-submit /opt/marketpulse/src/marketpulse/streaming/bronze_trades.py
docker compose exec -d spark spark-submit /opt/marketpulse/src/marketpulse/streaming/bronze_book_ticker.py
```

They trigger every 60 seconds, so **wait about two minutes** before expecting
rows — that interval is what keeps Parquet files at a workable size.

**Checkpoint** — this is the moment the platform is actually alive:

```powershell
docker compose exec -T trino trino --execute "select symbol, count(*) as trades, max(trade_time) as latest from lakehouse.bronze.trades group by 1 order by 2 desc"
```

*Make equivalent:* `make stream` and `make bronze`

---

## 6. Build the warehouse

```powershell
docker compose --profile core --profile processing --profile orchestration up -d --build
```

The image resolves dbt packages and compiles the manifest at build time, so
this step is slow once and fast afterwards. Then run the transformations:

```powershell
docker compose exec -T dagster-webserver dbt build --project-dir /opt/dagster/dbt/marketpulse --profiles-dir /opt/dagster/dbt/marketpulse
```

This runs 15 models, 1 seed, 1 snapshot and 80 tests in dependency order.
`build` rather than `run` then `test` on purpose: it interleaves them, so a
model whose upstream test failed is never built.

**Some tests will fail on a first run, and that is correct.** Freshness,
contiguity and reconciliation tests need history the platform does not have
yet — a few minutes of data cannot satisfy a 24-hour completeness check. They
turn green once the stack has been running for a day.

**Checkpoint:**

```powershell
docker compose exec -T trino trino --execute "select symbol, bar_start, close_price, vwap, order_flow_imbalance, time_weighted_spread_bps from lakehouse.gold.fct_market_1m order by bar_start desc limit 10"
```

*Make equivalent:* `make dbt-build`

---

## 7. The rest

```powershell
docker compose --profile observability up -d
docker compose --profile core --profile processing --profile serving up -d
```

Everything, once it is all up:

| | URL | Credentials |
|---|---|---|
| Dagster | http://localhost:3000 | — |
| Grafana | http://localhost:3001 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| Trino | http://localhost:8090 | — |
| Redpanda console | http://localhost:8080 | — |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Serving API | http://localhost:8000/docs | — |

In Dagster, **Assets → View global asset lineage** shows the whole graph from
Kafka topic to gold mart in one picture. That view is the point of ADR-0005.

*Make equivalent:* `make up-all`, and `make urls` prints this table.

---

## Everyday commands

```powershell
docker compose ps                                    # what is running
docker compose logs -f producer                      # follow the feed
docker compose exec -T redpanda rpk topic consume md.dead_letter.v1 --num 20
docker compose exec -it trino trino --catalog lakehouse --schema gold
docker compose --profile core --profile processing --profile ingestion --profile orchestration --profile serving --profile observability down
```

To stop everything but keep the data, use `down`. To destroy the local lake as
well, add `-v`.

---

## When something does not come up

**The Iceberg catalog exits with "No suitable driver found".** The image is
built locally for exactly this reason; make sure the command included
`--build`, or run `docker compose --profile core build iceberg-rest` first.

**An image tag fails to pull.** Registries retag and occasionally remove
versions. Every image is pinned in `docker-compose.yml`; bump the tag to a
neighbouring version and rebuild. The two most likely candidates are
`apache/iceberg-rest-fixture` and the `apache/spark` base in
`docker/spark/Dockerfile`.

**A container is `unhealthy` but its logs look fine.** Check memory first —
Docker Desktop's default allocation is well below what Spark and Trino need
together, and the OOM killer produces confusing symptoms rather than a clear
error.

**Redpanda works from a container but not from the host, or vice versa.** That
is the two-listener setup: `redpanda:9092` inside the compose network,
`localhost:19092` from the host. They are not interchangeable.

**No rows in bronze after two minutes.** In order: is the producer publishing
(`docker compose logs producer`), are the messages on the topic
(`rpk topic consume md.trades.v1 --num 5`), and is the Spark query alive
(`docker compose exec spark ps aux | Select-String bronze`). Each answer rules
out one hop.

Beyond that, [`runbook.md`](runbook.md) covers the failure modes with their
diagnosis and their fix.
