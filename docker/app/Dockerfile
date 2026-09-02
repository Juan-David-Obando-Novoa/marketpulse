# ---------------------------------------------------------------------------
# Application image: the producer, the CLI, the serving API and (as a separate
# target) the Dagster code location.
#
# Multi-stage with a shared builder so the three runtime images share layers,
# and so the compiler toolchain confluent-kafka needs to build never reaches a
# running container.
# ---------------------------------------------------------------------------
FROM python:3.11-slim-bookworm AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential librdkafka-dev git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip wheel --wheel-dir /wheels ".[ingestion,serving]"


FROM python:3.11-slim-bookworm AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        librdkafka1 curl \
    && rm -rf /var/lib/apt/lists/* \
    # Never run as root. The producer holds a public websocket open; there is
    # no reason for it to have any privilege at all.
    && useradd --create-home --uid 10001 marketpulse

COPY --from=builder /wheels /wheels
RUN pip install --no-index --find-links=/wheels marketpulse[ingestion,serving] && rm -rf /wheels

USER marketpulse
WORKDIR /home/marketpulse

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -sf http://localhost:9108/metrics >/dev/null || exit 1

ENTRYPOINT []
CMD ["marketpulse", "--help"]


FROM runtime AS orchestration

USER root
RUN pip install --no-cache-dir \
        "dagster>=1.7,<2" "dagster-webserver>=1.7,<2" "dagster-dbt>=0.23" \
        "dbt-trino>=1.8" "pyiceberg[s3fs]>=0.6" "trino>=0.328"

COPY orchestration /opt/dagster/app
COPY dbt /opt/dagster/dbt
RUN mkdir -p /opt/dagster/home && chown -R marketpulse /opt/dagster

ENV DAGSTER_HOME=/opt/dagster/home \
    DBT_PROJECT_DIR=/opt/dagster/dbt/marketpulse \
    DBT_PROFILES_DIR=/opt/dagster/dbt/marketpulse

USER marketpulse
WORKDIR /opt/dagster/app
HEALTHCHECK NONE
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]
