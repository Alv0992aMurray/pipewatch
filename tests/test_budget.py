"""Tests for pipewatch.budget alert budget tracking."""

from datetime import datetime, timedelta

import pytest

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.budget import AlertBudget, BudgetConfig


def _alert(pipeline: str = "orders", rule: str = "low_success") -> Alert:
    return Alert(
        pipeline=pipeline,
        rule_name=rule,
        severity=AlertSeverity.WARNING,
        message="Test alert",
        metric_value=0.5,
    )


@pytest.fixture
def config() -> BudgetConfig:
    return BudgetConfig(pipeline="orders", max_alerts=3, window_minutes=60)


@pytest.fixture
def budget(config: BudgetConfig) -> AlertBudget:
    return AlertBudget(config=config)


def _now() -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0)


def test_first_alert_is_allowed(budget: AlertBudget) -> None:
    result = budget.check(_alert(), now=_now())
    assert result.allowed is True


def test_alerts_within_budget_are_allowed(budget: AlertBudget) -> None:
    now = _now()
    for _ in range(3):
        result = budget.check(_alert(), now=now)
    assert result.allowed is True


def test_alert_beyond_budget_is_blocked(budget: AlertBudget) -> None:
    now = _now()
    for _ in range(3):
        budget.check(_alert(), now=now)
    result = budget.check(_alert(), now=now)
    assert result.allowed is False


def test_budget_remaining_decrements(budget: AlertBudget) -> None:
    now = _now()
    r1 = budget.check(_alert(), now=now)
    assert r1.budget_remaining == 2
    r2 = budget.check(_alert(), now=now)
    assert r2.budget_remaining == 1
    r3 = budget.check(_alert(), now=now)
    assert r3.budget_remaining == 0


def test_blocked_alert_has_zero_remaining(budget: AlertBudget) -> None:
    now = _now()
    for _ in range(3):
        budget.check(_alert(), now=now)
    result = budget.check(_alert(), now=now)
    assert result.budget_remaining == 0


def test_alerts_expire_after_window(budget: AlertBudget) -> None:
    now = _now()
    for _ in range(3):
        budget.check(_alert(), now=now)
    later = now + timedelta(minutes=61)
    result = budget.check(_alert(), now=later)
    assert result.allowed is True


def test_count_in_window_reflects_active_entries(budget: AlertBudget) -> None:
    now = _now()
    budget.check(_alert(), now=now)
    budget.check(_alert(), now=now)
    assert budget.count_in_window(now=now) == 2


def test_reset_clears_all_entries(budget: AlertBudget) -> None:
    now = _now()
    for _ in range(3):
        budget.check(_alert(), now=now)
    budget.reset()
    assert budget.count_in_window(now=now) == 0


def test_to_dict_contains_expected_keys(budget: AlertBudget) -> None:
    now = _now()
    result = budget.check(_alert(), now=now)
    d = result.to_dict()
    assert "pipeline" in d
    assert "allowed" in d
    assert "budget_remaining" in d
    assert "budget_limit" in d
