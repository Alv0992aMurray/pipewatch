"""Tests for pipewatch.signal."""
import pytest
from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.signal import compute_signal, _grade


def _alert(severity: AlertSeverity, pipeline: str = "pipe") -> Alert:
    return Alert(
        pipeline=pipeline,
        rule_name="test_rule",
        message="test",
        severity=severity,
        metric_value=0.5,
    )


def test_grade_boundaries():
    assert _grade(1.0) == "A"
    assert _grade(0.9) == "A"
    assert _grade(0.89) == "B"
    assert _grade(0.75) == "B"
    assert _grade(0.74) == "C"
    assert _grade(0.5) == "C"
    assert _grade(0.49) == "D"
    assert _grade(0.25) == "D"
    assert _grade(0.24) == "F"
    assert _grade(0.0) == "F"


def test_no_alerts_returns_perfect_score():
    result = compute_signal("pipe", [])
    assert result.score == 1.0
    assert result.grade == "A"
    assert result.alert_count == 0
    assert result.dominant_severity is None


def test_single_warning_deducts_correctly():
    result = compute_signal("pipe", [_alert(AlertSeverity.WARNING)])
    assert abs(result.score - 0.85) < 1e-6
    assert result.warning_count == 1
    assert result.critical_count == 0
    assert result.dominant_severity == AlertSeverity.WARNING.value


def test_single_critical_deducts_correctly():
    result = compute_signal("pipe", [_alert(AlertSeverity.CRITICAL)])
    assert abs(result.score - 0.65) < 1e-6
    assert result.critical_count == 1
    assert result.dominant_severity == AlertSeverity.CRITICAL.value


def test_mixed_alerts_deduct_combined():
    alerts = [_alert(AlertSeverity.CRITICAL), _alert(AlertSeverity.WARNING)]
    result = compute_signal("pipe", alerts)
    assert abs(result.score - 0.50) < 1e-6
    assert result.grade == "C"


def test_score_clamped_to_zero():
    alerts = [_alert(AlertSeverity.CRITICAL)] * 5
    result = compute_signal("pipe", alerts)
    assert result.score == 0.0
    assert result.grade == "F"


def test_pipeline_name_preserved():
    result = compute_signal("my_pipeline", [])
    assert result.pipeline == "my_pipeline"


def test_to_dict_keys():
    result = compute_signal("pipe", [_alert(AlertSeverity.WARNING)])
    d = result.to_dict()
    assert set(d.keys()) == {
        "pipeline", "score", "grade", "alert_count",
        "critical_count", "warning_count", "dominant_severity",
    }


def test_to_dict_score_rounded():
    result = compute_signal("pipe", [_alert(AlertSeverity.WARNING)])
    d = result.to_dict()
    assert isinstance(d["score"], float)
    assert len(str(d["score"]).split(".")[1]) <= 3
