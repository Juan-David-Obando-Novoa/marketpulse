"""Reconnect and retry policy.

A websocket to a public exchange endpoint *will* drop. The question is only
whether the reconnect storm that follows is polite. Full jitter (AWS's
recommendation) is used rather than plain exponential backoff, because every
replica reconnecting on the same doubling schedule is a self-inflicted DDoS.
"""

from __future__ import annotations

import random

__all__ = ["ExponentialBackoff"]


class ExponentialBackoff:
    """Exponential backoff with full jitter and an explicit reset.

    >>> backoff = ExponentialBackoff(initial=1.0, maximum=60.0, jitter=False)
    >>> [backoff.next_delay() for _ in range(4)]
    [1.0, 2.0, 4.0, 8.0]
    >>> backoff.reset()
    >>> backoff.next_delay()
    1.0
    """

    def __init__(
        self,
        *,
        initial: float = 1.0,
        maximum: float = 60.0,
        multiplier: float = 2.0,
        jitter: bool = True,
    ) -> None:
        if initial <= 0 or maximum < initial or multiplier <= 1:
            raise ValueError("require 0 < initial <= maximum and multiplier > 1")
        self._initial = initial
        self._maximum = maximum
        self._multiplier = multiplier
        self._jitter = jitter
        self._attempt = 0

    @property
    def attempt(self) -> int:
        """Number of delays handed out since the last :meth:`reset`."""
        return self._attempt

    def next_delay(self) -> float:
        """Return the next delay in seconds and advance the schedule."""
        ceiling = min(self._maximum, self._initial * self._multiplier**self._attempt)
        self._attempt += 1
        if not self._jitter:
            return ceiling
        return random.uniform(0.0, ceiling)  # noqa: S311 - not cryptographic

    def reset(self) -> None:
        """Call after a successful connection so the next drop starts cheap."""
        self._attempt = 0
