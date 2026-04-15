"""Tests for pipewatch.enrichment."""
from __future__ import annotations

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.enrichment import (
    EnrichmentRule,
    EnrichedAlert,
    EnrichmentResult,
    enrich_alerts,
)


def _alert(pipeline: str = "orders", severity: AlertSeverity = AlertSeverity.WARNING) -> Alert:
    return Alert(
        pipeline=pipeline,
        rule="low_success",
        severity=severity,
        message="Success rate below threshold",
        value=0.7,
    )


# --- EnrichmentRule.matches ---

def test_rule_with_no_filters_matches_any_alert():
    rule = EnrichmentRule(metadata={"team": "data-eng"})
    assert rule.matches(_alert("orders")) is True
    assert rule.matches(_alert("payments")) is True


def test_rule_with_pipeline_filter_matches_correct_pipeline():
    rule = EnrichmentRule(metadata={"team": "payments"}, pipeline="payments")
    assert rule.matches(_alert("payments")) is True
    assert rule.matches(_alert("orders")) is False


def test_rule_with_severity_filter_matches_correct_severity():
    rule = EnrichmentRule(metadata={"oncall": "yes"}, severity="critical")
    assert rule.matches(_alert(severity=AlertSeverity.CRITICAL)) is True
    assert rule.matches(_alert(severity=AlertSeverity.WARNING)) is False


def test_rule_with_both_filters_requires_both_to_match():
    rule = EnrichmentRule(metadata={"page": "true"}, pipeline="orders", severity="critical")
    assert rule.matches(_alert("orders", AlertSeverity.CRITICAL)) is True
    assert rule.matches(_alert("orders", AlertSeverity.WARNING)) is False
    assert rule.matches(_alert("payments", AlertSeverity.CRITICAL)) is False


# --- enrich_alerts ---

def test_enrich_alerts_no_rules_produces_empty_metadata():
    alerts = [_alert()]
    result = enrich_alerts(alerts, [])
    assert result.total == 1
    assert result.enriched[0].metadata == {}


def test_enrich_alerts_applies_matching_rule():
    alerts = [_alert("orders")]
    rules = [EnrichmentRule(metadata={"team": "data-eng"}, pipeline="orders")]
    result = enrich_alerts(alerts, rules)
    assert result.enriched[0].metadata == {"team": "data-eng"}


def test_enrich_alerts_merges_multiple_matching_rules():
    alerts = [_alert("orders", AlertSeverity.CRITICAL)]
    rules = [
        EnrichmentRule(metadata={"team": "data-eng"}),
        EnrichmentRule(metadata={"page": "true"}, severity="critical"),
    ]
    result = enrich_alerts(alerts, rules)
    assert result.enriched[0].metadata == {"team": "data-eng", "page": "true"}


def test_enrich_alerts_later_rule_overwrites_earlier_for_same_key():
    alerts = [_alert()]
    rules = [
        EnrichmentRule(metadata={"team": "alpha"}),
        EnrichmentRule(metadata={"team": "beta"}),
    ]
    result = enrich_alerts(alerts, rules)
    assert result.enriched[0].metadata["team"] == "beta"


def test_enrich_alerts_non_matching_rule_skipped():
    alerts = [_alert("orders")]
    rules = [EnrichmentRule(metadata={"team": "payments"}, pipeline="payments")]
    result = enrich_alerts(alerts, rules)
    assert result.enriched[0].metadata == {}


def test_enrich_alerts_empty_list_returns_empty_result():
    result = enrich_alerts([], [EnrichmentRule(metadata={"team": "data-eng"})])
    assert result.total == 0


# --- EnrichedAlert.to_dict ---

def test_to_dict_includes_metadata_when_present():
    ea = EnrichedAlert(alert=_alert(), metadata={"team": "data-eng"})
    d = ea.to_dict()
    assert d["metadata"] == {"team": "data-eng"}


def test_to_dict_omits_metadata_key_when_empty():
    ea = EnrichedAlert(alert=_alert(), metadata={})
    d = ea.to_dict()
    assert "metadata" not in d
