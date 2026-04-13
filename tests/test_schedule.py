"""Tests for pipewatch.schedule."""

from datetime import datetime, timedelta

import pytest

from pipewatch.schedule import (
    ScheduleConfig,
    ScheduleStatus,
    check_schedule,
)


@pytest.fixture
def config() -> ScheduleConfig:
    return ScheduleConfig(pipeline_name="orders", interval_minutes=60, grace_period_minutes=5)


def _now() -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0)


def test_on_time_within_grace(config):
    last_run = _now() - timedelta(minutes=63)  # interval=60, grace=5 → deadline at 65 min
    result = check_schedule(config, last_run, now=_now())
    assert result.status == ScheduleStatus.ON_TIME
    assert not result.is_overdue()


def test_late_after_grace_but_within_next_interval(config):
    last_run = _now() - timedelta(minutes=90)  # past grace (65 min) but < 2 intervals (120 min)
    result = check_schedule(config, last_run, now=_now())
    assert result.status == ScheduleStatus.LATE
    assert result.is_overdue()


def test_missing_beyond_two_intervals(config):
    last_run = _now() - timedelta(minutes=130)
    result = check_schedule(config, last_run, now=_now())
    assert result.status == ScheduleStatus.MISSING
    assert result.is_overdue()


def test_unknown_when_no_last_run(config):
    result = check_schedule(config, last_run=None, now=_now())
    assert result.status == ScheduleStatus.UNKNOWN
    assert not result.is_overdue()
    assert result.last_run is None
    assert result.expected_by is None


def test_expected_by_is_last_run_plus_interval(config):
    last_run = _now() - timedelta(minutes=30)
    result = check_schedule(config, last_run, now=_now())
    assert result.expected_by == last_run + timedelta(minutes=60)


def test_to_dict_keys(config):
    last_run = _now() - timedelta(minutes=30)
    result = check_schedule(config, last_run, now=_now())
    d = result.to_dict()
    assert set(d.keys()) == {"pipeline_name", "status", "last_run", "expected_by", "checked_at"}
    assert d["pipeline_name"] == "orders"
    assert d["status"] == ScheduleStatus.ON_TIME.value


def test_uses_utcnow_when_no_now_provided(config):
    """Smoke-test that check_schedule works without an explicit `now`."""
    last_run = datetime.utcnow() - timedelta(minutes=10)
    result = check_schedule(config, last_run)
    assert result.status in list(ScheduleStatus)
