"""Tests for alert inhibition logic."""
import pytest
from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.inhibition import InhibitionRule, apply_inhibition


def _alert(pipeline: str, severity: AlertSeverity, message: str = "msg") -> Alert:
    return Alert(pipeline=pipeline, severity=severity, message=message, rule_name="r")


@pytest.fixture
def source_alert():
    return _alert("db_loader", AlertSeverity.CRITICAL, "DB loader is down")


@pytest.fixture
def target_alert():
    return _alert("transform", AlertSeverity.WARNING, "Transform failing")


@pytest.fixture
def rule():
    return InhibitionRule(
        source_pipeline="db_loader",
        source_severity=AlertSeverity.CRITICAL,
        target_pipeline="*",
        reason="upstream failure",
    )


def test_no_rules_keeps_all_alerts(source_alert, target_alert):
    result = apply_inhibition([source_alert, target_alert], [])
    assert len(result.kept) == 2
    assert result.total_inhibited == 0


def test_target_inhibited_when_source_fires(source_alert, target_alert, rule):
    result = apply_inhibition([source_alert, target_alert], [rule])
    assert target_alert in result.inhibited
    assert source_alert in result.kept


def test_source_alert_is_not_self_inhibited(source_alert, rule):
    result = apply_inhibition([source_alert], [rule])
    assert source_alert in result.kept
    assert result.total_inhibited == 0


def test_no_inhibition_when_source_absent(target_alert, rule):
    result = apply_inhibition([target_alert], [rule])
    assert target_alert in result.kept
    assert result.total_inhibited == 0


def test_specific_target_pipeline_only_inhibits_named_pipeline():
    rule = InhibitionRule(
        source_pipeline="db_loader",
        source_severity=AlertSeverity.CRITICAL,
        target_pipeline="transform",
    )
    source = _alert("db_loader", AlertSeverity.CRITICAL)
    target_match = _alert("transform", AlertSeverity.WARNING)
    target_other = _alert("export", AlertSeverity.WARNING)

    result = apply_inhibition([source, target_match, target_other], [rule])
    assert target_match in result.inhibited
    assert target_other in result.kept
    assert source in result.kept


def test_to_dict_contains_expected_keys(source_alert, target_alert, rule):
    result = apply_inhibition([source_alert, target_alert], [rule])
    d = result.to_dict()
    assert "kept" in d
    assert "inhibited" in d
    assert "total_inhibited" in d
    assert d["total_inhibited"] == 1
