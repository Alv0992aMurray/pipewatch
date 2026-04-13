"""Tests for pipewatch.escalation_config."""
from pathlib import Path

import pytest
import yaml

from pipewatch.alerts import AlertSeverity
from pipewatch.escalation_config import EscalationConfigError, load_escalation_policies


def _write(tmp_path: Path, data: dict) -> Path:
    p = tmp_path / "escalations.yml"
    p.write_text(yaml.dump(data))
    return p


def test_load_returns_empty_when_file_missing(tmp_path):
    result = load_escalation_policies(tmp_path / "nonexistent.yml")
    assert result == []


def test_load_basic_policy(tmp_path):
    p = _write(
        tmp_path,
        {"escalations": [{"pipeline": "etl_main", "rule": "low_success"}]},
    )
    policies = load_escalation_policies(p)
    assert len(policies) == 1
    assert policies[0].pipeline == "etl_main"
    assert policies[0].rule_name == "low_success"


def test_load_default_threshold_and_severity(tmp_path):
    p = _write(
        tmp_path,
        {"escalations": [{"pipeline": "etl_main", "rule": "low_success"}]},
    )
    policy = load_escalation_policies(p)[0]
    assert policy.threshold == 3
    assert policy.escalate_to == AlertSeverity.CRITICAL


def test_load_custom_threshold(tmp_path):
    p = _write(
        tmp_path,
        {"escalations": [{"pipeline": "p", "rule": "r", "threshold": 5}]},
    )
    assert load_escalation_policies(p)[0].threshold == 5


def test_load_custom_severity(tmp_path):
    p = _write(
        tmp_path,
        {"escalations": [{"pipeline": "p", "rule": "r", "escalate_to": "warning"}]},
    )
    assert load_escalation_policies(p)[0].escalate_to == AlertSeverity.WARNING


def test_missing_pipeline_raises(tmp_path):
    p = _write(tmp_path, {"escalations": [{"rule": "low_success"}]})
    with pytest.raises(EscalationConfigError, match="pipeline"):
        load_escalation_policies(p)


def test_missing_rule_raises(tmp_path):
    p = _write(tmp_path, {"escalations": [{"pipeline": "p"}]})
    with pytest.raises(EscalationConfigError, match="rule"):
        load_escalation_policies(p)


def test_invalid_threshold_raises(tmp_path):
    p = _write(
        tmp_path,
        {"escalations": [{"pipeline": "p", "rule": "r", "threshold": 0}]},
    )
    with pytest.raises(EscalationConfigError, match="threshold"):
        load_escalation_policies(p)


def test_unknown_severity_raises(tmp_path):
    p = _write(
        tmp_path,
        {"escalations": [{"pipeline": "p", "rule": "r", "escalate_to": "extreme"}]},
    )
    with pytest.raises(EscalationConfigError, match="severity"):
        load_escalation_policies(p)


def test_empty_file_returns_empty(tmp_path):
    p = tmp_path / "e.yml"
    p.write_text("")
    assert load_escalation_policies(p) == []
