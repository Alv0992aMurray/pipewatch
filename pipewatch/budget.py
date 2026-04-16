"""Alert budget tracking: limits total alerts fired within a time window."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional

from pipewatch.alerts import Alert


@dataclass
class BudgetConfig:
    pipeline: str
    max_alerts: int
    window_minutes: int

    @property
    def window(self) -> timedelta:
        return timedelta(minutes=self.window_minutes)


@dataclass
class BudgetEntry:
    alert: Alert
    fired_at: datetime

    def to_dict(self) -> dict:
        return {
            "pipeline": self.alert.pipeline,
            "rule": self.alert.rule_name,
            "severity": self.alert.severity.value,
            "fired_at": self.fired_at.isoformat(),
        }


@dataclass
class BudgetResult:
    alert: Alert
    allowed: bool
    budget_remaining: int
    budget_limit: int

    def to_dict(self) -> dict:
        return {
            "pipeline": self.alert.pipeline,
            "allowed": self.allowed,
            "budget_remaining": self.budget_remaining,
            "budget_limit": self.budget_limit,
        }


@dataclass
class AlertBudget:
    config: BudgetConfig
    _log: List[BudgetEntry] = field(default_factory=list)

    def _prune(self, now: datetime) -> None:
        cutoff = now - self.config.window
        self._log = [e for e in self._log if e.fired_at > cutoff]

    def count_in_window(self, now: Optional[datetime] = None) -> int:
        now = now or datetime.utcnow()
        self._prune(now)
        return len(self._log)

    def check(self, alert: Alert, now: Optional[datetime] = None) -> BudgetResult:
        now = now or datetime.utcnow()
        self._prune(now)
        used = len(self._log)
        remaining = max(0, self.config.max_alerts - used)
        allowed = used < self.config.max_alerts
        if allowed:
            self._log.append(BudgetEntry(alert=alert, fired_at=now))
            remaining = max(0, self.config.max_alerts - len(self._log))
        return BudgetResult(
            alert=alert,
            allowed=allowed,
            budget_remaining=remaining,
            budget_limit=self.config.max_alerts,
        )

    def reset(self) -> None:
        self._log.clear()

    def summary(self, now: Optional[datetime] = None) -> dict:
        """Return a snapshot of the current budget state.

        Returns a dict with the pipeline name, how many alerts have been fired
        in the current window, the configured limit, and how many remain.
        """
        now = now or datetime.utcnow()
        used = self.count_in_window(now)
        return {
            "pipeline": self.config.pipeline,
            "window_minutes": self.config.window_minutes,
            "used": used,
            "limit": self.config.max_alerts,
            "remaining": max(0, self.config.max_alerts - used),
        }
