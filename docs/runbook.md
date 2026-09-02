# Runbook

What to do when something breaks, written for whoever is holding the pager and
has not read the codebase.

Every alert in `docker/prometheus/alerts.yml` links to a section here by
anchor. If you arrived from an alert, scroll to your section; the rest is
context you do not need right now.

**First, three commands that answer most questions:**

```bash
make ps                     # what is running
make urls                   # where everything lives
make dlq                    # what the pipeline is rejecting
```

---

## FeedSilent

> No market data from the venue for over two minutes.

**What it means.** The producer process is alive and the socket may well be
established, but nothing is arriving. This is the failure mode a message-rate
alert cannot see, because an idle socket produces a perfectly healthy zero.

**Why it is never a false positive.** Crypto venues trade continuously. There
is no market close, no weekend, and no holiday. Two minutes of silence on a
major pair is a fault, always.

**Diagnose, in order:**

```bash
# 1. Is the socket up at all? 0 means the reconnect loop is failing.
curl -s localhost:9108/metrics | grep marketpulse_feed_connected

# 2. Is the reconnect loop spinning? A climbing count means it connects and drops.
curl -s localhost:9108/metrics | grep marketpulse_feed_reconnects_total

# 3. What does the producer say?
docker compose logs --tail=100 producer
```

**Most likely causes, most likely first:**

1. **The venue is rate-limiting or geo-blocking us.** Look for `418` or `429` in
   the logs. A 418 is an IP ban and does not resolve by retrying — see
   [RateLimited](#ratelimited).
2. **DNS or egress.** `docker compose exec producer curl -sI https://data-api.binance.vision/api/v3/ping`
3. **The subscription was silently truncated.** If exactly one symbol is
   missing rather than all of them, check the stream count: the venue truncates
   an oversized subscription without complaint, which is why
   `build_stream_url` refuses to build one.

**Mitigate.** Restart the producer: `docker compose restart producer`. It
resumes from live; no offsets are lost, because the producer holds none. Bronze
is unaffected and the gap is real data loss for the silent window — record it,
because `mart_pipeline_health` will flag those minutes as incomplete and
somebody will ask.

---

## FeedDisconnected

> The feed has had no established socket for two minutes.

Distinct from `FeedSilent`: here the reconnect loop is actively failing rather
than succeeding into silence.

```bash
curl -s localhost:9108/metrics | grep -E 'marketpulse_feed_reconnects_total'
docker compose logs --tail=200 producer | grep feed.connection_lost
```

The `reason` label on the reconnect counter tells you which exception ended the
socket. `ConnectionError` is our own idle watchdog firing; anything else came
from the network or the venue.

If the reason is `ConnectionRefusedError` or a TLS error and the venue is
otherwise reachable from your laptop, the container's egress is the problem,
not the venue.

---

## IngestionLagHigh

> p99 lag from venue event time to our ingestion is above five seconds.

**Tell apart "the venue is slow" from "we are slow"** — this is the whole
diagnostic:

```promql
# Us: produce() to broker acknowledgement.
histogram_quantile(0.99, sum by (le) (rate(marketpulse_delivery_latency_seconds_bucket[5m])))
```

- Delivery latency **flat** while ingestion lag rises → the venue is behind, or
  our clock has drifted ahead of theirs. Check the recorded skew:
  `docker compose logs producer | grep clock_skew`.
- Delivery latency **rising too** → the broker or the network is the
  bottleneck. Check `marketpulse_producer_queue_depth`; a rising floor (not a
  spike) is backpressure.

**Mitigate.** Reduce the subscription (`MP_BINANCE__SYMBOLS`) to shed load, or
give the broker more resources. Do not raise the idle timeout to make the alert
quiet.

---

## DeadLetterRateElevated

> Sustained dead-lettering above 0.5/s.

**What it means.** Payloads are arriving that our contract rejects. A handful is
noise; a sustained rate is upstream schema drift.

```bash
# What is being rejected, and why.
make dlq

# Or by exception class, which is the low-cardinality label.
curl -s localhost:9108/metrics | grep marketpulse_dead_letters_total
```

**Read the `error_type`:**

| `error_type` | Meaning | Action |
|---|---|---|
| `NormalizationError` | The payload *shape* changed: a field vanished or changed type. | Compare the `raw_payload` in the DLQ against `contracts/schemas/`. This is a contract change; follow ADR-0006. |
| `ValidationError` | The shape is fine, the *values* are not — usually a crossed book. | If it is a burst during a dislocation, it is real and self-resolving. If it is sustained, the venue is publishing bad quotes. |
| `JSONDecodeError` | Truncated frames. | Almost always a network or proxy problem, not the venue. |

**Recovering dead-lettered messages.** The full original payload is in the DLQ
record. After fixing the contract, replay:

```bash
docker compose exec redpanda rpk topic consume md.dead_letter.v1 --num 1000 > dlq.json
# fix the contract, redeploy, then re-publish the extracted raw_payload values
```

Nothing is lost while the DLQ retains them (7 days).

---

## VenueSequenceRegressions

> A symbol's venue update id repeatedly failed to advance.

**What it means.** The venue's `update_id` is monotonic per symbol, so an id
that does not move forward is a replay or an out-of-order delivery.

**What it does NOT mean.** It is not a message-loss metric, and there is
deliberately no alert on *gaps* in that id. For the bookTicker stream `u` is
the order book's update id: it advances at every depth while a top-of-book
message is only emitted when the touch moves, so gaps between consecutive
quotes are normal and constant. An earlier version of this platform alerted on
them and fired continuously from the first minute of real data.

**Diagnose:**

```bash
curl -s localhost:9108/metrics | grep marketpulse_sequence_regressions_total
docker compose logs --tail=100 producer | grep sequence_regression
```

A burst right after a reconnect is expected and self-resolving — the tracker
resets its high-water mark, but a replay in flight can still land. Sustained
regressions with no reconnects mean messages are genuinely arriving out of
order, which on a single-partition-per-symbol topic points at the producer, not
the broker.

**Where loss would actually show.** `slv_quote_metrics_1m.quote_coverage_ratio`
below 1 for a minute means we held no quote for part of it, and
`mart_pipeline_health` flags the day `degraded_liquidity_view`. That is the
signal to trust.

## RateLimited

> `418` or `429` in the backfill logs.

**429** means slow down. The client already honours `Retry-After` and
self-throttles at 75% of the published weight budget, so a sustained 429 means
something else is sharing our source IP.

**418** means we ignored a 429 and are now IP-banned for a period the venue
chooses. **Do not retry.** The client escalates rather than retrying for exactly
this reason.

```bash
# What the venue thinks we have consumed this minute.
docker compose logs producer | grep rest.self_throttling
```

**Mitigate.** Stop all backfills, wait out the ban (the `Retry-After` header
states the duration), then resume with a narrower date range. Live streaming
uses a different endpoint and is usually unaffected.

---

## Streaming query stopped

> `bronze_streaming_queries` asset failing, or the bronze high-water mark not
> advancing.

```bash
# Is the query alive?
docker compose exec spark ps aux | grep bronze

# Is it landing rows? This is the honest signal; process liveness is not.
make sql
> select max(trade_time), count(*) from lakehouse.bronze.trades;
```

**Restarting is safe.** Offsets live in the Spark checkpoint, not in a consumer
group, so a restart resumes exactly where it stopped:

```bash
make bronze
```

**If it crash-loops on the first batch after a long outage**, it is trying to
consume the whole retention window at once. `maxOffsetsPerTrigger` bounds this,
but a multi-day outage can still overwhelm it. Restart with an explicit offset:

```bash
docker compose exec spark spark-submit \
  /opt/marketpulse/src/marketpulse/streaming/bronze_trades.py latest
```

You will lose the intervening messages from the stream — recover them from the
venue's REST endpoint with a backfill instead.

**If it fails with `failOnDataLoss`**, Kafka retention expired before the job
caught up. That error is deliberately not suppressed: setting
`failOnDataLoss=false` converts a loud "you lost data" into a silent gap. Note
the affected window, restart at `latest`, and backfill.

---

## Decode quarantine is not empty

> `check_no_decode_quarantine` failing.

**This is a contract violation, not a data-quality blip.** ADR-0006 promises
BACKWARD compatibility: if a message cannot be decoded by the current reader
schema, that promise was broken somewhere.

```sql
select _kafka_topic, _kafka_partition, _kafka_offset, count(*)
from lakehouse.ops.decode_quarantine
where _ingested_at >= current_timestamp - interval '24' hour
group by 1, 2, 3;
```

The Kafka coordinates identify the exact messages. Read them off the topic to
see what was actually written:

```bash
docker compose exec redpanda rpk topic consume md.trades.v1 \
  --partition 0 --offset <offset> --num 1
```

Then check whether the schema registered under that subject matches what is in
`contracts/schemas/`. A mismatch means a producer was deployed with a schema
that CI did not gate — find out how, because that is the more important bug.

---

## dbt build failing

```bash
cd dbt/marketpulse
dbt build --select <failing_model>+ --debug
```

**Failing *tests* rather than failing models:** `store_failures` is on, so the
offending rows are in a table rather than needing to be re-derived:

```sql
select * from lakehouse.dbt_test_failures.<test_name> limit 100;
```

**Common causes, in the order they actually occur:**

| Symptom | Cause | Fix |
|---|---|---|
| `unique_combination_of_columns` fails on a silver model | The dedup lookback is narrower than the replay window | Raise `dedup_lookback_hours` in `dbt_project.yml` and full-refresh the model |
| `assert_no_overlapping_fx_intervals` fails | The publisher issued a correction | Full-refresh `slv_fx_rates`; the model rebuilds bounds from successors |
| `assert_candles_are_contiguous` fails | Real ingestion gap | Cross-check against `FeedSilent` for the same window |
| `vwap between low_price and high_price` fails | The weighting is wrong | A genuine bug in `slv_ohlcv_1m`; do not widen the test |
| Reconciliation below 98% | We are missing trades the venue printed | Backfill the affected window, then re-run |

**Never widen a test to make it pass.** Every threshold in this project has a
comment saying what it is protecting against; if it fires, either the data is
wrong or the comment is.

---

## Trino queries suddenly slow

Almost always small files. Check before assuming anything else:

```sql
select count(*) as data_files
from lakehouse.bronze."trades$files";
```

A healthy `bronze.trades` holds tens to low hundreds of files per day. Thousands
means maintenance has not run.

```bash
make maintenance
```

If maintenance itself is failing, run it standalone — it needs neither Trino
nor Spark, which is the point:

```bash
docker compose exec spark python3 -m marketpulse.maintenance.iceberg_maintenance \
  --table bronze.trades --dry-run
```

---

## Backfilling

Backfills are idempotent by construction: the window comes from the arguments,
never from `now()`, so re-running the same command produces the same rows and
the silver dedup absorbs the overlap.

**One symbol, one range:**

```bash
make backfill SYMBOL=BTCUSDT START=2026-03-01 END=2026-03-08
```

**Many partitions:** use Dagster, which is what the partitioning is for —
select the `binance_klines_backfill` asset, choose the symbol and date range,
and launch. It respects the retry policy and the rate limiter.

**After any backfill**, rebuild the downstream warehouse:

```bash
make dbt-build
```

The incremental models reconsider a 24-hour lookback window by default. For a
backfill older than that, full-refresh the affected models:

```bash
cd dbt/marketpulse && dbt build --select slv_ohlcv_1m+ --full-refresh
```

---

## Full local recovery

When the local stack is in an unknown state and you want a clean slate. **This
deletes all local data.**

```bash
make nuke
make up
make bootstrap
make up-all
```

Then let the streaming jobs run for a few minutes and confirm:

```bash
make sql
> select symbol, count(*), max(trade_time) from lakehouse.bronze.trades group by 1;
```
