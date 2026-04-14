"""Tests for pipewatch.routing."""
from __future__ import annotations

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.routing import RoutingRule, RoutingResult, route_alerts


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def warning_alert() -> Alert:
    return Alert(pipeline="orders", rule="low_success", message="low", severity=AlertSeverity.WARNING)


@pytest.fixture()
def critical_alert() -> Alert:
    return Alert(pipeline="payments", rule="high_error", message="high", severity=AlertSeverity.CRITICAL)


# ---------------------------------------------------------------------------
# RoutingRule.matches
# ---------------------------------------------------------------------------

def test_rule_matches_any_pipeline_when_none(warning_alert):
    rule = RoutingRule(destination="slack", pipeline=None, min_severity=AlertSeverity.WARNING)
    assert rule.matches(warning_alert) is True


def test_rule_rejects_wrong_pipeline(warning_alert):
    rule = RoutingRule(destination="slack", pipeline="payments", min_severity=AlertSeverity.WARNING)
    assert rule.matches(warning_alert) is False


def test_rule_rejects_below_min_severity(warning_alert):
    rule = RoutingRule(destination="pagerduty", min_severity=AlertSeverity.CRITICAL)
    assert rule.matches(warning_alert) is False


def test_rule_accepts_exact_severity(critical_alert):
    rule = RoutingRule(destination="pagerduty", min_severity=AlertSeverity.CRITICAL)
    assert rule.matches(critical_alert) is True


def test_rule_accepts_higher_severity(critical_alert):
    rule = RoutingRule(destination="slack", min_severity=AlertSeverity.WARNING)
    assert rule.matches(critical_alert) is True


# ---------------------------------------------------------------------------
# route_alerts
# ---------------------------------------------------------------------------

def test_route_empty_alerts():
    results = route_alerts([], [RoutingRule(destination="slack")])
    assert results == []


def test_route_single_alert_to_one_destination(warning_alert):
    rules = [RoutingRule(destination="slack", min_severity=AlertSeverity.WARNING)]
    results = route_alerts([warning_alert], rules)
    assert len(results) == 1
    assert results[0].destinations == ["slack"]
    assert results[0].routed is True


def test_route_alert_to_multiple_destinations(critical_alert):
    rules = [
        RoutingRule(destination="slack", min_severity=AlertSeverity.WARNING),
        RoutingRule(destination="pagerduty", min_severity=AlertSeverity.CRITICAL),
    ]
    results = route_alerts([critical_alert], rules)
    assert set(results[0].destinations) == {"slack", "pagerduty"}


def test_unrouted_alert_has_empty_destinations(warning_alert):
    rules = [RoutingRule(destination="pagerduty", min_severity=AlertSeverity.CRITICAL)]
    results = route_alerts([warning_alert], rules)
    assert results[0].routed is False
    assert results[0].destinations == []


def test_to_dict_contains_expected_keys(warning_alert):
    result = RoutingResult(alert=warning_alert, destinations=["slack"])
    d = result.to_dict()
    assert d["pipeline"] == "orders"
    assert d["destinations"] == ["slack"]
    assert d["routed"] is True
