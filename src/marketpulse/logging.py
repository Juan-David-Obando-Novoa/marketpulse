"""Structured logging.

Every log line is a JSON object with a stable set of keys, because the first
thing anyone does with pipeline logs is grep them by ``symbol`` or ``topic``
and the second thing is ship them to a log store that expects structure.

Local development gets a human-readable console renderer instead; the choice is
driven by configuration, not by guessing at ``isatty``.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

__all__ = ["bind_context", "configure_logging", "get_logger"]

_CONFIGURED = False


def configure_logging(
    *,
    level: str = "INFO",
    json_logs: bool = True,
    service_name: str = "marketpulse",
) -> None:
    """Configure structlog and the stdlib root logger exactly once.

    Idempotent: importing two modules that both call this is harmless, which
    matters because Spark executors and Dagster processes have different
    entry points into the same library code.
    """
    # Module-level flag rather than an object: configure_logging is called
    # from several entry points (CLI, Spark executor, Dagster op) and must
    # be idempotent across all of them without any one of them owning it.
    global _CONFIGURED  # noqa: PLW0603
    if _CONFIGURED:
        return

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    renderer: Any = (
        structlog.processors.JSONRenderer()
        if json_logs
        else structlog.dev.ConsoleRenderer(colors=True)
    )

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level.upper())),
        # The stdlib factory (rather than PrintLoggerFactory) is required by
        # add_logger_name, and means library logs and ours share one sink --
        # which matters inside Spark and Dagster, where third-party logging
        # is configured by the host process and we only get to add to it.
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=logging.getLevelName(level.upper()),
    )
    # Third-party libraries are chatty at INFO; they are useful at WARNING.
    for noisy in ("websockets", "aiohttp", "urllib3", "botocore", "s3transfer", "py4j"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    structlog.contextvars.bind_contextvars(service=service_name)
    _CONFIGURED = True


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a bound logger. Call after :func:`configure_logging`."""
    return structlog.get_logger(name)


def bind_context(**kwargs: Any) -> None:
    """Bind key/values onto every subsequent log line in this async context."""
    structlog.contextvars.bind_contextvars(**kwargs)
