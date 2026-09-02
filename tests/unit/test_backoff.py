"""Backoff shape matters: it is the difference between a reconnect and a stampede."""

from __future__ import annotations

import pytest

from marketpulse.utils.backoff import ExponentialBackoff

pytestmark = pytest.mark.unit


def test_delays_double_without_jitter() -> None:
    backoff = ExponentialBackoff(initial=0.5, maximum=100.0, jitter=False)
    assert [backoff.next_delay() for _ in range(5)] == [0.5, 1.0, 2.0, 4.0, 8.0]


def test_delay_is_capped_at_maximum() -> None:
    backoff = ExponentialBackoff(initial=1.0, maximum=8.0, jitter=False)
    delays = [backoff.next_delay() for _ in range(10)]
    assert max(delays) == 8.0
    assert delays[-1] == 8.0


def test_reset_returns_to_the_initial_delay() -> None:
    backoff = ExponentialBackoff(initial=1.0, maximum=60.0, jitter=False)
    for _ in range(5):
        backoff.next_delay()
    backoff.reset()
    assert backoff.attempt == 0
    assert backoff.next_delay() == 1.0


def test_jitter_stays_within_the_deterministic_ceiling() -> None:
    """Full jitter samples [0, ceiling); it must never exceed the ceiling."""
    backoff = ExponentialBackoff(initial=1.0, maximum=4.0, jitter=True)
    for attempt in range(6):
        ceiling = min(4.0, 1.0 * 2**attempt)
        assert 0.0 <= backoff.next_delay() <= ceiling


def test_jitter_actually_varies() -> None:
    """A 'jittered' backoff that returns a constant is the bug this catches."""
    backoff = ExponentialBackoff(initial=10.0, maximum=10.0, jitter=True)
    samples = {backoff.next_delay() for _ in range(50)}
    assert len(samples) > 1


@pytest.mark.parametrize(
    "kwargs",
    [
        {"initial": 0.0},
        {"initial": -1.0},
        {"initial": 10.0, "maximum": 1.0},
        {"multiplier": 1.0},
    ],
)
def test_invalid_configuration_is_rejected_at_construction(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError, match="require"):
        ExponentialBackoff(**kwargs)  # type: ignore[arg-type]
