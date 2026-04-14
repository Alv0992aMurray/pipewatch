"""Tests for pipewatch.audit."""

from datetime import datetime, timezone

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.audit import AuditEntry, AuditLog, build_audit_entry, audit_log_to_jsonl
from pipewatch.metrics import PipelineMetric


@pytest.fixture
def healthy_metric():
    return PipelineMetric(
        pipeline_name="orders",
        total_rows=1000,
        failed_rows=5,
        error_rate=0.005,
        latency_seconds=1.2,
    )


@pytest.fixture
def failing_metric():
    return PipelineMetric(
        pipeline_name="payments",
        total_rows=200,
        failed_rows=80,
        error_rate=0.4,
        latency_seconds=9.5,
    )


@pytest.fixture
def sample_alert():
    return Alert(
        pipeline="payments",
        rule_name="low_success",
        severity=AlertSeverity.CRITICAL,
        message="Success rate below threshold",
        value=0.6,
        threshold=0.9,
    )


def test_build_audit_entry_healthy_no_alerts(healthy_metric):
    ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    entry = build_audit_entry(healthy_metric, [], checked_at=ts)

    assert entry.pipeline == "orders"
    assert entry.total_rows == 1000
    assert entry.failed_rows == 5
    assert entry.is_healthy is True
    assert entry.alert_count == 0
    assert entry.alert_summaries == []
    assert entry.checked_at == ts


def test_build_audit_entry_failing_with_alerts(failing_metric, sample_alert):
    entry = build_audit_entry(failing_metric, [sample_alert])

    assert entry.pipeline == "payments"
    assert entry.is_healthy is False
    assert entry.alert_count == 1
    assert "Success rate below threshold" in entry.alert_summaries


def test_audit_entry_to_dict(healthy_metric):
    ts = datetime(2024, 6, 15, 8, 30, 0, tzinfo=timezone.utc)
    entry = build_audit_entry(healthy_metric, [], checked_at=ts)
    d = entry.to_dict()

    assert d["pipeline"] == "orders"
    assert d["checked_at"] == ts.isoformat()
    assert d["alert_count"] == 0
    assert isinstance(d["success_rate"], float)


def test_audit_log_record_and_last_n(healthy_metric, failing_metric):
    log = AuditLog()
    e1 = build_audit_entry(healthy_metric, [])
    e2 = build_audit_entry(failing_metric, [])
    log.record(e1)
    log.record(e2)

    assert log.total_runs() == 2
    assert log.last_n(1) == [e2]
    assert log.last_n(2) == [e1, e2]


def test_audit_log_entries_for_pipeline(healthy_metric, failing_metric):
    log = AuditLog()
    log.record(build_audit_entry(healthy_metric, []))
    log.record(build_audit_entry(failing_metric, []))
    log.record(build_audit_entry(healthy_metric, []))

    orders_entries = log.entries_for("orders")
    assert len(orders_entries) == 2
    assert all(e.pipeline == "orders" for e in orders_entries)


def test_audit_log_unhealthy_runs(failing_metric, healthy_metric):
    log = AuditLog()
    log.record(build_audit_entry(healthy_metric, []))
    log.record(build_audit_entry(failing_metric, []))
    log.record(build_audit_entry(failing_metric, []))

    assert log.unhealthy_runs() == 2


def test_audit_log_to_jsonl(healthy_metric, failing_metric):
    import json

    log = AuditLog()
    log.record(build_audit_entry(healthy_metric, []))
    log.record(build_audit_entry(failing_metric, []))

    jsonl = audit_log_to_jsonl(log)
    lines = jsonl.strip().split("\n")
    assert len(lines) == 2
    first = json.loads(lines[0])
    assert first["pipeline"] == "orders"
