"""Alert muting: temporarily mute alerts for a pipeline/rule combination."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from pipewatch.alerts import Alert


@dataclass
class MuteRule:
    pipeline: Optional[str]  # None = wildcard
    rule_name: Optional[str]  # None = wildcard
    expires_at: datetime
    reason: str = ""

    def is_active(self, now: Optional[datetime] = None) -> bool:
        now = now or datetime.utcnow()
        return now < self.expires_at

    def matches(self, alert: Alert) -> bool:
        if self.pipeline is not None and alert.pipeline != self.pipeline:
            return False
        if self.rule_name is not None and alert.rule_name != self.rule_name:
            return False
        return True

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline or "*",
            "rule_name": self.rule_name or "*",
            "expires_at": self.expires_at.isoformat(),
            "reason": self.reason,
            "active": self.is_active(),
        }


@dataclass
class MuteResult:
    kept: List[Alert] = field(default_factory=list)
    muted: List[Alert] = field(default_factory=list)

    @property
    def total_muted(self) -> int:
        return len(self.muted)

    def to_dict(self) -> dict:
        return {
            "kept": [a.to_dict() for a in self.kept],
            "muted": [a.to_dict() for a in self.muted],
            "total_muted": self.total_muted,
        }


def apply_mutes(
    alerts: List[Alert],
    rules: List[MuteRule],
    now: Optional[datetime] = None,
) -> MuteResult:
    now = now or datetime.utcnow()
    active_rules = [r for r in rules if r.is_active(now)]
    result = MuteResult()
    for alert in alerts:
        if any(r.matches(alert) for r in active_rules):
            result.muted.append(alert)
        else:
            result.kept.append(alert)
    return result
