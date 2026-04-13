"""Deduplication of alerts — suppress repeated firing of the same alert within a cooldown window."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.alerts import Alert


@dataclass
class DedupEntry:
    pipeline: str
    rule_name: str
    first_seen: float
    last_seen: float
    count: int = 1

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "rule_name": self.rule_name,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "count": self.count,
        }


@dataclass
class DedupResult:
    kept: List[Alert] = field(default_factory=list)
    suppressed: List[Alert] = field(default_factory=list)

    @property
    def total_suppressed(self) -> int:
        return len(self.suppressed)


class AlertDeduplicator:
    """Tracks recently fired alerts and suppresses duplicates within a cooldown period."""

    def __init__(self, cooldown_seconds: float = 300.0) -> None:
        self.cooldown_seconds = cooldown_seconds
        self._seen: Dict[str, DedupEntry] = {}

    def _key(self, alert: Alert) -> str:
        return f"{alert.pipeline}::{alert.rule_name}"

    def _now(self) -> float:  # pragma: no cover — overridden in tests
        return time.time()

    def process(self, alerts: List[Alert]) -> DedupResult:
        result = DedupResult()
        now = self._now()
        for alert in alerts:
            key = self._key(alert)
            entry = self._seen.get(key)
            if entry is None or (now - entry.last_seen) >= self.cooldown_seconds:
                self._seen[key] = DedupEntry(
                    pipeline=alert.pipeline,
                    rule_name=alert.rule_name,
                    first_seen=now,
                    last_seen=now,
                )
                result.kept.append(alert)
            else:
                entry.last_seen = now
                entry.count += 1
                result.suppressed.append(alert)
        return result

    def entries(self) -> List[DedupEntry]:
        return list(self._seen.values())

    def clear(self) -> None:
        self._seen.clear()
