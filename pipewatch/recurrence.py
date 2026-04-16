"""Recurrence detection: identify alerts that fire repeatedly across runs."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from pipewatch.alerts import Alert


@dataclass
class RecurrenceEntry:
    pipeline: str
    rule_name: str
    occurrences: int
    first_seen: datetime
    last_seen: datetime

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "rule_name": self.rule_name,
            "occurrences": self.occurrences,
            "first_seen": self.first_seen.isoformat(),
            "last_seen": self.last_seen.isoformat(),
        }


@dataclass
class RecurrenceResult:
    entries: List[RecurrenceEntry] = field(default_factory=list)

    @property
    def total_recurring(self) -> int:
        return sum(1 for e in self.entries if e.occurrences > 1)

    @property
    def frequent_entries(self) -> List[RecurrenceEntry]:
        return [e for e in self.entries if e.occurrences > 1]

    def to_dict(self) -> dict:
        return {
            "total_recurring": self.total_recurring,
            "entries": [e.to_dict() for e in self.entries],
        }


@dataclass
class RecurrenceTracker:
    _store: dict = field(default_factory=dict)

    def _key(self, alert: Alert) -> str:
        return f"{alert.pipeline}::{alert.rule_name}"

    def record(self, alert: Alert, ts: Optional[datetime] = None) -> None:
        now = ts or datetime.utcnow()
        key = self._key(alert)
        if key in self._store:
            entry = self._store[key]
            entry.occurrences += 1
            entry.last_seen = now
        else:
            self._store[key] = RecurrenceEntry(
                pipeline=alert.pipeline,
                rule_name=alert.rule_name,
                occurrences=1,
                first_seen=now,
                last_seen=now,
            )

    def evaluate(self, alerts: List[Alert], ts: Optional[datetime] = None) -> RecurrenceResult:
        for alert in alerts:
            self.record(alert, ts)
        return RecurrenceResult(entries=list(self._store.values()))

    def reset(self, pipeline: str, rule_name: str) -> None:
        key = f"{pipeline}::{rule_name}"
        self._store.pop(key, None)
