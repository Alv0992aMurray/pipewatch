"""Dead-letter queue for alerts that failed all delivery attempts."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.alerts import Alert


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class DeadLetterEntry:
    alert: Alert
    reason: str
    attempts: int
    first_failed_at: datetime
    last_failed_at: datetime

    def to_dict(self) -> dict:
        return {
            "pipeline": self.alert.pipeline,
            "rule": self.alert.rule_name,
            "severity": self.alert.severity.value,
            "reason": self.reason,
            "attempts": self.attempts,
            "first_failed_at": self.first_failed_at.isoformat(),
            "last_failed_at": self.last_failed_at.isoformat(),
        }


@dataclass
class DeadLetterQueue:
    _entries: List[DeadLetterEntry] = field(default_factory=list)

    def push(self, alert: Alert, reason: str, attempts: int = 1) -> DeadLetterEntry:
        now = _now()
        entry = DeadLetterEntry(
            alert=alert,
            reason=reason,
            attempts=attempts,
            first_failed_at=now,
            last_failed_at=now,
        )
        self._entries.append(entry)
        return entry

    def all(self) -> List[DeadLetterEntry]:
        return list(self._entries)

    def count(self) -> int:
        return len(self._entries)

    def drain(self) -> List[DeadLetterEntry]:
        drained = list(self._entries)
        self._entries.clear()
        return drained

    def find(self, pipeline: str) -> List[DeadLetterEntry]:
        return [e for e in self._entries if e.alert.pipeline == pipeline]

    def most_recent(self) -> Optional[DeadLetterEntry]:
        return self._entries[-1] if self._entries else None
