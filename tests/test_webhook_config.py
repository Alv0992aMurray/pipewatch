"""Tests for pipewatch.webhook_config."""
from __future__ import annotations

import os

import pytest
import yaml

from pipewatch.alerts import AlertSeverity
from pipewatch.webhook_config import WebhookConfigError, load_webhook_configs


@pytest.fixture
def write_yaml(tmp_path):
    def _write(data: dict) -> str:
        p = tmp_path / "webhooks.yml"
        p.write_text(yaml.dump(data))
        return str(p)
    return _write


def test_load_returns_empty_when_file_missing(tmp_path):
    result = load_webhook_configs(str(tmp_path / "nonexistent.yml"))
    assert result == []


def test_load_basic_webhook(write_yaml):
    path = write_yaml({"webhooks": [{"url": "http://example.com/hook"}]})
    configs = load_webhook_configs(path)
    assert len(configs) == 1
    assert configs[0].url == "http://example.com/hook"
    assert configs[0].min_severity == AlertSeverity.WARNING


def test_load_custom_severity(write_yaml):
    path = write_yaml({"webhooks": [{"url": "http://x.com", "min_severity": "critical"}]})
    configs = load_webhook_configs(path)
    assert configs[0].min_severity == AlertSeverity.CRITICAL


def test_load_custom_headers(write_yaml):
    path = write_yaml({"webhooks": [{"url": "http://x.com", "headers": {"X-Token": "abc"}}]})
    configs = load_webhook_configs(path)
    assert configs[0].headers == {"X-Token": "abc"}


def test_load_custom_timeout(write_yaml):
    path = write_yaml({"webhooks": [{"url": "http://x.com", "timeout": 30}]})
    configs = load_webhook_configs(path)
    assert configs[0].timeout == 30


def test_missing_url_raises(write_yaml):
    path = write_yaml({"webhooks": [{"min_severity": "warning"}]})
    with pytest.raises(WebhookConfigError, match="url"):
        load_webhook_configs(path)


def test_invalid_severity_raises(write_yaml):
    path = write_yaml({"webhooks": [{"url": "http://x.com", "min_severity": "extreme"}]})
    with pytest.raises(WebhookConfigError, match="severity"):
        load_webhook_configs(path)


def test_empty_file_returns_empty(write_yaml):
    path = write_yaml({})
    result = load_webhook_configs(path)
    assert result == []


def test_load_multiple_webhooks(write_yaml):
    """Verify that multiple webhook entries are all loaded correctly."""
    path = write_yaml({
        "webhooks": [
            {"url": "http://first.com/hook", "min_severity": "warning"},
            {"url": "http://second.com/hook", "min_severity": "critical"},
        ]
    })
    configs = load_webhook_configs(path)
    assert len(configs) == 2
    assert configs[0].url == "http://first.com/hook"
    assert configs[0].min_severity == AlertSeverity.WARNING
    assert configs[1].url == "http://second.com/hook"
    assert configs[1].min_severity == AlertSeverity.CRITICAL
