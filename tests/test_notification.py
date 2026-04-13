"""Tests for pipewatch.notification and pipewatch.notification_reporter."""
from __future__ import annotations

import json
import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.notification import (
    NotificationChannel,
    NotificationResult,
    route_alerts,
)
from pipewatch.notification_reporter import (
    format_notification_result,
    notification_report_to_json,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def warning_alert() -> Alert:
    return Alert(
        pipeline="orders",
        message="success rate low",
        severity=AlertSeverity.WARNING,
        value=0.7,
        threshold=0.9,
    )


@pytest.fixture()
def critical_alert() -> Alert:
    return Alert(
        pipeline="payments",
        message="error rate critical",
        severity=AlertSeverity.CRITICAL,
        value=0.4,
        threshold=0.1,
    )


# ---------------------------------------------------------------------------
# NotificationChannel.accepts
# ---------------------------------------------------------------------------

def test_channel_accepts_alert_at_min_severity(warning_alert):
    ch = NotificationChannel(name="slack", min_severity=AlertSeverity.WARNING)
    assert ch.accepts(warning_alert) is True


def test_channel_rejects_alert_below_min_severity(warning_alert):
    ch = NotificationChannel(name="pagerduty", min_severity=AlertSeverity.CRITICAL)
    assert ch.accepts(warning_alert) is False


def test_channel_accepts_critical_when_min_is_warning(critical_alert):
    ch = NotificationChannel(name="slack", min_severity=AlertSeverity.WARNING)
    assert ch.accepts(critical_alert) is True


def test_channel_pipeline_filter_matches(warning_alert):
    ch = NotificationChannel(name="orders-team", pipeline_filter="orders")
    assert ch.accepts(warning_alert) is True


def test_channel_pipeline_filter_rejects_other_pipeline(warning_alert):
    ch = NotificationChannel(name="payments-team", pipeline_filter="payments")
    assert ch.accepts(warning_alert) is False


# ---------------------------------------------------------------------------
# route_alerts
# ---------------------------------------------------------------------------

def test_route_alerts_empty_alerts():
    ch = NotificationChannel(name="slack")
    result = route_alerts([], [ch])
    assert result.total_routed() == 0
    assert result.channels_with_alerts() == []


def test_route_alerts_sends_to_matching_channel(warning_alert, critical_alert):
    slack = NotificationChannel(name="slack", min_severity=AlertSeverity.WARNING)
    pagerduty = NotificationChannel(name="pagerduty", min_severity=AlertSeverity.CRITICAL)
    result = route_alerts([warning_alert, critical_alert], [slack, pagerduty])

    assert len(result.routed["slack"]) == 2
    assert len(result.routed["pagerduty"]) == 1
    assert result.routed["pagerduty"][0].pipeline == "payments"


def test_route_alerts_channels_with_alerts(warning_alert, critical_alert):
    slack = NotificationChannel(name="slack", min_severity=AlertSeverity.WARNING)
    pagerduty = NotificationChannel(name="pagerduty", min_severity=AlertSeverity.CRITICAL)
    result = route_alerts([warning_alert], [slack, pagerduty])
    assert "slack" in result.channels_with_alerts()
    assert "pagerduty" not in result.channels_with_alerts()


# ---------------------------------------------------------------------------
# reporter
# ---------------------------------------------------------------------------

def test_format_notification_result_no_alerts():
    result = NotificationResult(routed={"slack": []})
    text = format_notification_result(result)
    assert "No alerts routed" in text


def test_format_notification_result_shows_channel_and_alert(warning_alert):
    result = NotificationResult(routed={"slack": [warning_alert]})
    text = format_notification_result(result)
    assert "slack" in text
    assert "orders" in text
    assert "WARNING" in text


def test_notification_report_to_json(warning_alert):
    result = NotificationResult(routed={"slack": [warning_alert]})
    data = json.loads(notification_report_to_json(result))
    assert "slack" in data
    assert data["slack"][0]["pipeline"] == "orders"
