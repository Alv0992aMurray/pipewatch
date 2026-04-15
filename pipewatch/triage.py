"""Alert triage: prioritise and classify alerts by urgency and impact."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.alerts import Alert, AlertSeverity


@dataclass
class TriageEntry:
    alert: Alert
    priority: int          # lower = more urgent
    category: str          # 'data_quality' | 'throughput' | 'availability' | 'other'
    suggested_action: str

    def to_dict(self) -> dict:
        return {
            "pipeline": self.alert.pipeline,
            "rule": self.alert.rule_name,
            "severity": self.alert.severity.value,
            "priority": self.priority,
            "category": self.category,
            "suggested_action": self.suggested_action,
        }


@dataclass
class TriageResult:
    entries: List[TriageEntry] = field(default_factory=list)

    def sorted_entries(self) -> List[TriageEntry]:
        """Return entries ordered by priority (ascending)."""
        return sorted(self.entries, key=lambda e: e.priority)

    def critical_entries(self) -> List[TriageEntry]:
        return [
            e for e in self.entries
            if e.alert.severity == AlertSeverity.CRITICAL
        ]

    def to_dict(self) -> dict:
        return {"triage": [e.to_dict() for e in self.sorted_entries()]}


_CATEGORY_KEYWORDS = {
    "data_quality": {"success", "error", "fail", "quality"},
    "throughput": {"throughput", "rows", "processed", "rate"},
    "availability": {"latency", "lag", "delay", "missing", "schedule"},
}

_ACTIONS = {
    "data_quality": "Inspect source data and transformation logic for errors.",
    "throughput": "Check upstream data volume and pipeline capacity.",
    "availability": "Verify pipeline schedule and upstream availability.",
    "other": "Review pipeline logs and alert configuration.",
}


def _classify(alert: Alert) -> str:
    name_lower = alert.rule_name.lower()
    for category, keywords in _CATEGORY_KEYWORDS.items():
        if any(kw in name_lower for kw in keywords):
            return category
    return "other"


def _priority(alert: Alert, category: str) -> int:
    """Compute an integer priority (1 = most urgent)."""
    base = 1 if alert.severity == AlertSeverity.CRITICAL else 3
    # availability issues bump priority slightly
    offset = 0 if category == "availability" else 1
    return base + offset


def triage_alerts(alerts: List[Alert]) -> TriageResult:
    """Classify and prioritise a list of alerts."""
    result = TriageResult()
    for alert in alerts:
        category = _classify(alert)
        priority = _priority(alert, category)
        action = _ACTIONS[category]
        result.entries.append(
            TriageEntry(
                alert=alert,
                priority=priority,
                category=category,
                suggested_action=action,
            )
        )
    return result
