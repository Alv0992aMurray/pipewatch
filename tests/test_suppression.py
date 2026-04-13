"""Tests for pipewatch.suppression."""

from __future__ import annotations

import time
import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.suppression import SuppressionRule, apply_suppressions


NOW = 1_700_000_000.0
FUTURE = NOW + 3600
PAST = NOW - 1


def _alert(pipeline: str = "etl", rule_name: str = "low_success") -> Alert:
    return Alert(
        pipeline=pipeline,
        rule_name=rule_name,
        message="something went wrong",
        severity=AlertSeverity.WARNING,
    )


# --- SuppressionRule.is_active ---

def test_rule_is_active_before_expiry():
    rule = SuppressionRule(pipeline="etl", rule_name="low_success", until=FUTURE)
    assert rule.is_active(now=NOW) is True


def test_rule_is_not_active_after_expiry():
    rule = SuppressionRule(pipeline="etl", rule_name="low_success", until=PAST)
    assert rule.is_active(now=NOW) is False


# --- SuppressionRule.matches ---

def test_exact_match_suppresses_alert():
    rule = SuppressionRule(pipeline="etl", rule_name="low_success", until=FUTURE)
    assert rule.matches(_alert("etl", "low_success"), now=NOW) is True


def test_wildcard_pipeline_matches_any_pipeline():
    rule = SuppressionRule(pipeline="*", rule_name="low_success", until=FUTURE)
    assert rule.matches(_alert("other_pipeline", "low_success"), now=NOW) is True


def test_wildcard_rule_matches_any_rule():
    rule = SuppressionRule(pipeline="etl", rule_name="*", until=FUTURE)
    assert rule.matches(_alert("etl", "high_errors"), now=NOW) is True


def test_expired_rule_does_not_match():
    rule = SuppressionRule(pipeline="etl", rule_name="low_success", until=PAST)
    assert rule.matches(_alert("etl", "low_success"), now=NOW) is False


def test_wrong_pipeline_does_not_match():
    rule = SuppressionRule(pipeline="other", rule_name="low_success", until=FUTURE)
    assert rule.matches(_alert("etl", "low_success"), now=NOW) is False


# --- apply_suppressions ---

def test_no_rules_keeps_all_alerts():
    alerts = [_alert(), _alert("other", "rule")]
    result = apply_suppressions(alerts, [], now=NOW)
    assert len(result.kept) == 2
    assert len(result.suppressed) == 0


def test_matching_rule_suppresses_alert():
    rule = SuppressionRule(pipeline="etl", rule_name="low_success", until=FUTURE)
    alerts = [_alert("etl", "low_success"), _alert("etl", "high_errors")]
    result = apply_suppressions(alerts, [rule], now=NOW)
    assert len(result.suppressed) == 1
    assert result.suppressed[0].rule_name == "low_success"
    assert len(result.kept) == 1
    assert result.kept[0].rule_name == "high_errors"


def test_expired_rule_keeps_all_alerts():
    rule = SuppressionRule(pipeline="etl", rule_name="low_success", until=PAST)
    alerts = [_alert("etl", "low_success")]
    result = apply_suppressions(alerts, [rule], now=NOW)
    assert len(result.kept) == 1
    assert len(result.suppressed) == 0


def test_wildcard_suppresses_all_alerts_for_pipeline():
    rule = SuppressionRule(pipeline="etl", rule_name="*", until=FUTURE)
    alerts = [_alert("etl", "rule_a"), _alert("etl", "rule_b"), _alert("other", "rule_a")]
    result = apply_suppressions(alerts, [rule], now=NOW)
    assert len(result.suppressed) == 2
    assert len(result.kept) == 1
