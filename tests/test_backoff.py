"""Tests for pipewatch.backoff."""
from datetime import datetime, timedelta

import pytest

from pipewatch.backoff import AlertBackoffManager, BackoffConfig


@pytest.fixture
def config() -> BackoffConfig:
    return BackoffConfig(
        base_delay_seconds=60,
        max_delay_seconds=3600,
        multiplier=2.0,
        max_attempts=8,
    )


@pytest.fixture
def manager(config: BackoffConfig) -> AlertBackoffManager:
    return AlertBackoffManager(config)


def _now() -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0)


def test_first_alert_is_always_allowed(manager):
    result = manager.check("pipe_a", "low_success", now=_now())
    assert result.allowed is True


def test_first_alert_records_attempt(manager):
    result = manager.check("pipe_a", "low_success", now=_now())
    assert result.attempts == 1


def test_immediate_retry_is_blocked(manager):
    now = _now()
    manager.check("pipe_a", "low_success", now=now)
    result = manager.check("pipe_a", "low_success", now=now + timedelta(seconds=10))
    assert result.allowed is False


def test_retry_allowed_after_delay(manager):
    now = _now()
    manager.check("pipe_a", "low_success", now=now)
    # base delay is 60s; after first attempt delay = 60 * 2^0 = 60s
    result = manager.check("pipe_a", "low_success", now=now + timedelta(seconds=61))
    assert result.allowed is True


def test_delay_doubles_on_successive_attempts(manager):
    now = _now()
    # attempt 1 -> delay 60s
    manager.check("pipe_a", "low_success", now=now)
    # attempt 2 -> delay 120s
    t1 = now + timedelta(seconds=61)
    manager.check("pipe_a", "low_success", now=t1)
    # attempt 3 should require 120s from t1
    blocked = manager.check("pipe_a", "low_success", now=t1 + timedelta(seconds=60))
    assert blocked.allowed is False
    allowed = manager.check("pipe_a", "low_success", now=t1 + timedelta(seconds=121))
    assert allowed.allowed is True


def test_delay_capped_at_max(manager):
    now = _now()
    t = now
    # exhaust several attempts to hit the cap
    for i in range(8):
        result = manager.check("pipe_a", "low_success", now=t)
        assert result.allowed is True
        t = result.next_allowed + timedelta(seconds=1)
    entries = manager.entries()
    assert len(entries) == 1
    delay = entries[0].next_allowed - entries[0].last_fired
    assert delay.total_seconds() <= 3600


def test_different_rules_are_tracked_independently(manager):
    now = _now()
    manager.check("pipe_a", "rule_x", now=now)
    result = manager.check("pipe_a", "rule_y", now=now)
    assert result.allowed is True


def test_reset_clears_entry(manager):
    now = _now()
    manager.check("pipe_a", "low_success", now=now)
    manager.reset("pipe_a", "low_success")
    result = manager.check("pipe_a", "low_success", now=now + timedelta(seconds=1))
    assert result.allowed is True
    assert result.attempts == 1


def test_to_dict_contains_expected_keys(manager):
    result = manager.check("pipe_a", "low_success", now=_now())
    d = result.to_dict()
    assert "pipeline" in d
    assert "rule" in d
    assert "allowed" in d
    assert "attempts" in d
    assert "next_allowed" in d
