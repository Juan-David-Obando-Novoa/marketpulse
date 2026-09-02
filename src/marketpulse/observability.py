"""Prometheus instrumentation for the ingestion tier.

The metric set is chosen to answer the four questions an on-call engineer
actually asks, in the order they ask them:

1. *Is data flowing?*            ``marketpulse_messages_received_total``
2. *Is it reaching Kafka?*       ``marketpulse_messages_published_total``
3. *How stale is it?*            ``marketpulse_event_lag_seconds``
4. *What is being dropped?*      ``marketpulse_dead_letters_total``

Label cardinality is treated as a budget, not an afterthought. ``symbol`` and
``stream`` are bounded by configuration; ``error_type`` is an exception class
name. Free-text error messages, trade ids and timestamps are never labels --
that is how a Prometheus instance gets taken down by its own telemetry.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

if TYPE_CHECKING:
    from marketpulse.config import ObservabilitySettings

__all__ = [
    "IngestionMetrics",
    "start_metrics_server",
]

#: Lag buckets in seconds. Dense below one second because that is the healthy
#: regime and the interesting question is *which* percentile crossed 100ms;
#: sparse above ten seconds because past that the only question is "how bad".
_LAG_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0)

#: Kafka delivery-acknowledgement latency; the tail is what matters.
_DELIVERY_BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0)


class IngestionMetrics:
    """Metric handles for one producer process.

    Takes its own :class:`CollectorRegistry` rather than using the global
    default so that tests can assert on counter values without leaking state
    between test cases, and so two producers in one process (the FX poller and
    the websocket client during a CLI run) do not collide on registration.
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry if registry is not None else CollectorRegistry()

        self.messages_received = Counter(
            "marketpulse_messages_received_total",
            "Raw messages read off the upstream feed, before validation.",
            labelnames=("source", "stream"),
            registry=self.registry,
        )
        self.messages_published = Counter(
            "marketpulse_messages_published_total",
            "Messages acknowledged by the broker.",
            labelnames=("topic", "symbol"),
            registry=self.registry,
        )
        self.dead_letters = Counter(
            "marketpulse_dead_letters_total",
            "Messages routed to the dead-letter topic, by exception class.",
            labelnames=("origin_topic", "error_type"),
            registry=self.registry,
        )
        self.delivery_failures = Counter(
            "marketpulse_delivery_failures_total",
            "Broker-side delivery failures reported to the producer callback.",
            labelnames=("topic", "reason"),
            registry=self.registry,
        )
        self.reconnects = Counter(
            "marketpulse_feed_reconnects_total",
            "Upstream feed reconnections, by the reason the socket dropped.",
            labelnames=("source", "reason"),
            registry=self.registry,
        )
        self.sequence_gaps = Counter(
            "marketpulse_sequence_gaps_total",
            "Detected gaps in a venue's monotonic update sequence. "
            "Non-zero means we lost messages the venue believes it sent.",
            labelnames=("source", "symbol"),
            registry=self.registry,
        )

        self.event_lag = Histogram(
            "marketpulse_event_lag_seconds",
            "Venue event timestamp to local ingestion timestamp.",
            labelnames=("source", "stream"),
            buckets=_LAG_BUCKETS,
            registry=self.registry,
        )
        self.delivery_latency = Histogram(
            "marketpulse_delivery_latency_seconds",
            "Time from produce() to broker acknowledgement.",
            labelnames=("topic",),
            buckets=_DELIVERY_BUCKETS,
            registry=self.registry,
        )

        self.feed_connected = Gauge(
            "marketpulse_feed_connected",
            "1 while the upstream socket is established, 0 otherwise.",
            labelnames=("source",),
            registry=self.registry,
        )
        self.producer_queue_depth = Gauge(
            "marketpulse_producer_queue_depth",
            "Messages buffered in librdkafka awaiting delivery. "
            "A rising floor here is backpressure, not a spike.",
            labelnames=("client_id",),
            registry=self.registry,
        )
        self.last_message_timestamp = Gauge(
            "marketpulse_last_message_unixtime",
            "Unix time of the most recent message accepted from the feed. "
            "Alert on 'now() - this', which catches a silently idle socket "
            "that a rate counter alone would report as a legitimate zero.",
            labelnames=("source", "stream"),
            registry=self.registry,
        )

    def observe_lag(self, source: str, stream: str, lag_millis: int) -> None:
        """Record producer-side lag, clamping the negative values venues emit.

        Exchange clocks run slightly ahead of ours often enough that a raw
        negative observation is noise, not signal; clamping keeps the histogram
        honest without hiding real staleness.
        """
        self.event_lag.labels(source=source, stream=stream).observe(max(lag_millis, 0) / 1_000)


def start_metrics_server(settings: ObservabilitySettings, registry: CollectorRegistry) -> bool:
    """Expose ``/metrics`` on the configured port. Returns whether it started."""
    if not settings.metrics_enabled:
        return False
    start_http_server(settings.metrics_port, registry=registry)
    return True
