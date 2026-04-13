"""Tests for pipewatch.webhook."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.webhook import (
    WebhookConfig,
    WebhookResult,
    _build_payload,
    _filter_alerts,
    send_webhook,
)


@pytest.fixture
def warning_alert():
    return Alert(pipeline="pipe1", metric="success_rate", severity=AlertSeverity.WARNING,
                 message="Low success rate", value=0.7, threshold=0.9)


@pytest.fixture
def critical_alert():
    return Alert(pipeline="pipe2", metric="error_rate", severity=AlertSeverity.CRITICAL,
                 message="High error rate", value=0.5, threshold=0.1)


def test_filter_alerts_warning_includes_both(warning_alert, critical_alert):
    result = _filter_alerts([warning_alert, critical_alert], AlertSeverity.WARNING)
    assert len(result) == 2


def test_filter_alerts_critical_only(warning_alert, critical_alert):
    result = _filter_alerts([warning_alert, critical_alert], AlertSeverity.CRITICAL)
    assert result == [critical_alert]


def test_build_payload_contains_alerts(warning_alert):
    raw = json.loads(_build_payload([warning_alert]))
    assert "alerts" in raw
    assert raw["alerts"][0]["pipeline"] == "pipe1"
    assert raw["alerts"][0]["severity"] == "warning"


def test_send_webhook_no_relevant_alerts(warning_alert):
    config = WebhookConfig(url="http://example.com", min_severity=AlertSeverity.CRITICAL)
    result = send_webhook(config, [warning_alert])
    assert result.success is True
    assert result.status_code is None


def test_send_webhook_success(critical_alert):
    config = WebhookConfig(url="http://example.com/hook", min_severity=AlertSeverity.CRITICAL)
    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    with patch("urllib.request.urlopen", return_value=mock_resp):
        result = send_webhook(config, [critical_alert])
    assert result.success is True
    assert result.status_code == 200


def test_send_webhook_http_error(critical_alert):
    import urllib.error
    config = WebhookConfig(url="http://example.com/hook")
    with patch("urllib.request.urlopen", side_effect=urllib.error.HTTPError(
            url="", code=500, msg="Server Error", hdrs=None, fp=None)):
        result = send_webhook(config, [critical_alert])
    assert result.success is False
    assert result.status_code == 500


def test_send_webhook_connection_error(critical_alert):
    config = WebhookConfig(url="http://bad-host/hook")
    with patch("urllib.request.urlopen", side_effect=OSError("connection refused")):
        result = send_webhook(config, [critical_alert])
    assert result.success is False
    assert "connection refused" in result.error
