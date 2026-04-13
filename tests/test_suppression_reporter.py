"""Tests for pipewatch.suppression_reporter."""

from __future__ import annotations

import json
import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.suppression import SuppressionResult, SuppressionRule
from pipewatch.suppression_reporter import (
    format_suppression_result,
    format_suppression_rules,
    suppression_report_to_json,
)

NOW = 1_700_000_000.0
FUTURE = NOW + 3600


def _alert(pipeline="etl", rule_name="low_success", severity=AlertSeverity.WARNING):
    return Alert(pipeline=pipeline, rule_name=rule_name, message="test msg", severity=severity)


# --- format_suppression_result ---

def test_format_result_no_alerts():
    result = SuppressionResult()
    out = format_suppression_result(result)
    assert "No alerts" in out


def test_format_result_shows_suppressed():
    result = SuppressionResult(suppressed=[_alert()])
    out = format_suppression_result(result)
    assert "Suppressed" in out
    assert "low_success" in out


def test_format_result_shows_kept():
    result = SuppressionResult(kept=[_alert(rule_name="high_errors")])
    out = format_suppression_result(result)
    assert "Active alerts" in out
    assert "high_errors" in out


# --- format_suppression_rules ---

def test_format_rules_empty():
    out = format_suppression_rules([])
    assert "No active" in out


def test_format_rules_shows_rule():
    rule = SuppressionRule(pipeline="etl", rule_name="low_success", until=FUTURE, reason="maintenance")
    out = format_suppression_rules([rule])
    assert "etl" in out
    assert "low_success" in out
    assert "maintenance" in out


# --- suppression_report_to_json ---

def test_json_output_structure():
    result = SuppressionResult(
        kept=[_alert(rule_name="rule_a")],
        suppressed=[_alert(rule_name="rule_b")],
    )
    data = json.loads(suppression_report_to_json(result))
    assert len(data["kept"]) == 1
    assert len(data["suppressed"]) == 1
    assert data["kept"][0]["rule_name"] == "rule_a"
    assert data["suppressed"][0]["rule_name"] == "rule_b"


def test_json_empty_result():
    data = json.loads(suppression_report_to_json(SuppressionResult()))
    assert data["kept"] == []
    assert data["suppressed"] == []
