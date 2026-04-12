"""Tests for loading alert rules from YAML configuration."""

from pathlib import Path

import pytest

from pipewatch.alert_config import AlertConfigError, load_rules
from pipewatch.alerts import AlertSeverity


FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def write_yaml(tmp_path: Path, content: str) -> Path:
    cfg = tmp_path / "alerts.yml"
    cfg.write_text(content)
    return cfg


# ---------------------------------------------------------------------------
# Happy-path tests
# ---------------------------------------------------------------------------

def test_load_rules_from_project_config():
    """Smoke test: default config ships with valid rules."""
    rules = load_rules(Path("pipewatch_alerts.yml"))
    assert len(rules) >= 1
    assert all(r.metric in {"success_rate", "throughput", "error_count"} for r in rules)


def test_load_rules_parses_severity(tmp_path):
    cfg = write_yaml(tmp_path, """
rules:
  - name: test_rule
    metric: success_rate
    threshold: 0.9
    operator: lt
    severity: critical
""")
    rules = load_rules(cfg)
    assert rules[0].severity == AlertSeverity.CRITICAL


def test_load_rules_default_severity_is_warning(tmp_path):
    cfg = write_yaml(tmp_path, """
rules:
  - name: test_rule
    metric: throughput
    threshold: 5
    operator: lt
""")
    rules = load_rules(cfg)
    assert rules[0].severity == AlertSeverity.WARNING


def test_load_rules_custom_message(tmp_path):
    cfg = write_yaml(tmp_path, """
rules:
  - name: msg_rule
    metric: error_count
    threshold: 100
    operator: gt
    message: "Too many errors!"
""")
    rules = load_rules(cfg)
    assert rules[0].message == "Too many errors!"


# ---------------------------------------------------------------------------
# Error-path tests
# ---------------------------------------------------------------------------

def test_missing_config_raises(tmp_path):
    with pytest.raises(AlertConfigError, match="not found"):
        load_rules(tmp_path / "nonexistent.yml")


def test_missing_required_field_raises(tmp_path):
    cfg = write_yaml(tmp_path, """
rules:
  - name: incomplete_rule
    metric: success_rate
    threshold: 0.9
""")
    with pytest.raises(AlertConfigError, match="operator"):
        load_rules(cfg)


def test_invalid_metric_raises(tmp_path):
    cfg = write_yaml(tmp_path, """
rules:
  - name: bad_metric
    metric: banana
    threshold: 1
    operator: lt
""")
    with pytest.raises(AlertConfigError, match="banana"):
        load_rules(cfg)


def test_invalid_operator_raises(tmp_path):
    cfg = write_yaml(tmp_path, """
rules:
  - name: bad_op
    metric: success_rate
    threshold: 0.9
    operator: eq
""")
    with pytest.raises(AlertConfigError, match="eq"):
        load_rules(cfg)


def test_invalid_severity_raises(tmp_path):
    cfg = write_yaml(tmp_path, """
rules:
  - name: bad_sev
    metric: success_rate
    threshold: 0.9
    operator: lt
    severity: extreme
""")
    with pytest.raises(AlertConfigError, match="extreme"):
        load_rules(cfg)
