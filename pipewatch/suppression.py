"""Alert suppression rules — silence alerts matching criteria for a given window."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.alerts import Alert, AlertSeverity


@dataclass
class SuppressionRule:
    """Defines a window during which matching alerts are suppressed."""

    pipeline: str  # pipeline name glob; '*' matches all
    rule_name: str  # alert rule name to suppress; '*' matches all
    until: float  # unix timestamp when suppression expires
    reason: str = ""

    def is_active(self, now: Optional[float] = None) -> bool:
        """Return True if the suppression window is still open."""
        now = now if now is not None else time.time()
        return now < self.until

    def matches(self, alert: Alert, now: Optional[float] = None) -> bool:
        """Return True if this rule suppresses the given alert."""
        if not self.is_active(now):
            return False
        pipeline_match = self.pipeline == "*" or alert.pipeline == self.pipeline
        rule_match = self.rule_name == "*" or alert.rule_name == self.rule_name
        return pipeline_match and rule_match

    def to_dict(self) -> Dict:
        return {
            "pipeline": self.pipeline,
            "rule_name": self.rule_name,
            "until": self.until,
            "reason": self.reason,
        }


@dataclass
class SuppressionResult:
    """Outcome of applying suppression rules to a list of alerts."""

    kept: List[Alert] = field(default_factory=list)
    suppressed: List[Alert] = field(default_factory=list)


def apply_suppressions(
    alerts: List[Alert],
    rules: List[SuppressionRule],
    now: Optional[float] = None,
) -> SuppressionResult:
    """Filter alerts through suppression rules, returning kept and suppressed."""
    result = SuppressionResult()
    for alert in alerts:
        if any(r.matches(alert, now) for r in rules):
            result.suppressed.append(alert)
        else:
            result.kept.append(alert)
    return result
