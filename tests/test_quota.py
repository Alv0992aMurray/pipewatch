"""Tests for pipewatch.quota."""
from datetime import datetime, timedelta

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.quota import AlertQuotaManager, QuotaConfig


def _alert(pipeline: str = "etl", rule: str = "low_success") -> Alert:
    return Alert(
        pipeline=pipeline,
        rule_name=rule,
        severity=AlertSeverity.WARNING,
        message="test",
        value=0.5,
    )


@pytest.fixture
def config() -> QuotaConfig:
    return QuotaConfig(pipeline="etl", max_alerts=3, window_minutes=60)


@pytest.fixture
def manager() -> AlertQuotaManager:
    return AlertQuotaManager()


def _now() -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0)


def test_first_alert_is_kept(manager, config):
    result = manager.apply([_alert()], config, now=_now())
    assert len(result.kept) == 1
    assert result.total_dropped == 0


def test_alerts_within_quota_are_kept(manager, config):
    now = _now()
    alerts = [_alert() for _ in range(3)]
    result = manager.apply(alerts, config, now=now)
    assert len(result.kept) == 3
    assert result.total_dropped == 0


def test_alerts_exceeding_quota_are_dropped(manager, config):
    now = _now()
    alerts = [_alert() for _ in range(5)]
    result = manager.apply(alerts, config, now=now)
    assert len(result.kept) == 3
    assert result.total_dropped == 2


def test_alerts_for_other_pipelines_are_unaffected(manager, config):
    now = _now()
    alerts = [_alert(pipeline="other") for _ in range(10)]
    result = manager.apply(alerts, config, now=now)
    assert len(result.kept) == 10
    assert result.total_dropped == 0


def test_old_entries_are_pruned_allowing_new_alerts(manager, config):
    old = _now() - timedelta(hours=2)
    # fill quota at old time
    manager.apply([_alert() for _ in range(3)], config, now=old)
    # new window — quota should reset
    result = manager.apply([_alert() for _ in range(3)], config, now=_now())
    assert len(result.kept) == 3
    assert result.total_dropped == 0


def test_mixed_pipeline_alerts_only_quota_own_pipeline(manager, config):
    now = _now()
    alerts = [_alert("etl")] * 4 + [_alert("other")] * 2
    result = manager.apply(alerts, config, now=now)
    etl_kept = [a for a in result.kept if a.pipeline == "etl"]
    other_kept = [a for a in result.kept if a.pipeline == "other"]
    assert len(etl_kept) == 3
    assert len(other_kept) == 2
    assert result.total_dropped == 1
