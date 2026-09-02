"""Time conversion is where timezone bugs are born. These tests are the fence."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from marketpulse.utils.timeutil import (
    datetime_to_epoch_millis,
    day_range,
    epoch_millis_to_datetime,
    floor_to_interval,
    utc_now,
)

UTC = timezone.utc
pytestmark = pytest.mark.unit


def test_epoch_millis_round_trip() -> None:
    millis = 1_773_500_966_535
    assert datetime_to_epoch_millis(epoch_millis_to_datetime(millis)) == millis


def test_epoch_millis_result_is_timezone_aware() -> None:
    assert epoch_millis_to_datetime(0).tzinfo is not None


def test_negative_epoch_is_rejected() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        epoch_millis_to_datetime(-1)


def test_naive_datetime_is_rejected_rather_than_assumed_utc() -> None:
    """A silent UTC assumption is how an offset bug reaches production."""
    with pytest.raises(ValueError, match="naive datetime"):
        datetime_to_epoch_millis(datetime(2026, 1, 1))  # noqa: DTZ001


def test_non_utc_input_is_converted_not_truncated() -> None:
    bogota = timezone(timedelta(hours=-5))
    local = datetime(2026, 3, 14, 10, 9, 26, tzinfo=bogota)
    assert epoch_millis_to_datetime(datetime_to_epoch_millis(local)).hour == 15


@pytest.mark.parametrize(
    ("interval", "expected_minute", "expected_second"),
    [
        (timedelta(minutes=1), 9, 0),
        (timedelta(minutes=5), 5, 0),
        (timedelta(hours=1), 0, 0),
    ],
)
def test_floor_to_interval(interval: timedelta, expected_minute: int, expected_second: int) -> None:
    moment = datetime(2026, 3, 14, 15, 9, 26, 535000, tzinfo=UTC)
    floored = floor_to_interval(moment, interval)
    assert (floored.minute, floored.second, floored.microsecond) == (
        expected_minute,
        expected_second,
        0,
    )


def test_floor_is_idempotent() -> None:
    moment = datetime(2026, 3, 14, 15, 9, 26, tzinfo=UTC)
    once = floor_to_interval(moment, timedelta(minutes=5))
    assert floor_to_interval(once, timedelta(minutes=5)) == once


def test_floor_rejects_non_positive_interval() -> None:
    with pytest.raises(ValueError, match="positive"):
        floor_to_interval(utc_now(), timedelta(0))


def test_day_range_is_inclusive_on_both_ends() -> None:
    days = day_range(date(2026, 2, 26), date(2026, 3, 2))
    assert days[0] == date(2026, 2, 26)
    assert days[-1] == date(2026, 3, 2)
    assert len(days) == 5, "2026 is not a leap year; February has 28 days"


def test_day_range_rejects_inverted_bounds() -> None:
    with pytest.raises(ValueError, match="precedes"):
        day_range(date(2026, 3, 2), date(2026, 3, 1))
