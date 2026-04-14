"""Tests for pipewatch.routing_config."""
from __future__ import annotations

from pathlib import Path

import pytest

from pipewatch.alerts import AlertSeverity
from pipewatch.routing_config import RoutingConfigError, load_routing_rules


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "routing.yml"
    p.write_text(content)
    return p


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_missing_file_returns_empty_list(tmp_path):
    rules = load_routing_rules(tmp_path / "nonexistent.yml")
    assert rules == []


def test_empty_file_returns_empty_list(tmp_path):
    p = _write(tmp_path, "")
    assert load_routing_rules(p) == []


def test_load_basic_routing_rule(tmp_path):
    p = _write(tmp_path, "routing:\n  - destination: slack\n")
    rules = load_routing_rules(p)
    assert len(rules) == 1
    assert rules[0].destination == "slack"


def test_load_default_severity_is_warning(tmp_path):
    p = _write(tmp_path, "routing:\n  - destination: slack\n")
    rules = load_routing_rules(p)
    assert rules[0].min_severity == AlertSeverity.WARNING


def test_load_custom_severity(tmp_path):
    p = _write(tmp_path, "routing:\n  - destination: pagerduty\n    min_severity: critical\n")
    rules = load_routing_rules(p)
    assert rules[0].min_severity == AlertSeverity.CRITICAL


def test_load_pipeline_filter(tmp_path):
    p = _write(tmp_path, "routing:\n  - destination: slack\n    pipeline: orders\n")
    rules = load_routing_rules(p)
    assert rules[0].pipeline == "orders"


def test_missing_destination_raises(tmp_path):
    p = _write(tmp_path, "routing:\n  - min_severity: warning\n")
    with pytest.raises(RoutingConfigError, match="destination"):
        load_routing_rules(p)


def test_unknown_severity_raises(tmp_path):
    p = _write(tmp_path, "routing:\n  - destination: slack\n    min_severity: extreme\n")
    with pytest.raises(RoutingConfigError, match="severity"):
        load_routing_rules(p)


def test_load_multiple_rules(tmp_path):
    content = (
        "routing:\n"
        "  - destination: slack\n"
        "    min_severity: warning\n"
        "  - destination: pagerduty\n"
        "    min_severity: critical\n"
    )
    p = _write(tmp_path, content)
    rules = load_routing_rules(p)
    assert len(rules) == 2
    assert rules[1].destination == "pagerduty"
