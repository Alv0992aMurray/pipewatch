"""Alert quota enforcement — cap total alerts per pipeline per time window."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List

from pipewatch.alerts import Alert


@dataclass
class QuotaConfig:
    pipeline: str
    max_alerts: int
    window_minutes: int = 60

    @property
    def window(self) -> timedelta:
        return timedelta(minutes=self.window_minutes)


@dataclass
class QuotaEntry:
    pipeline: str
    timestamp: datetime

    def to_dict(self) -> dict:
        return {"pipeline": self.pipeline, "timestamp": self.timestamp.isoformat()}


@dataclass
class QuotaResult:
    kept: List[Alert] = field(default_factory=list)
    dropped: List[Alert] = field(default_factory=list)

    @property
    def total_dropped(self) -> int:
        return len(self.dropped)


@dataclass
class AlertQuotaManager:
    _entries: List[QuotaEntry] = field(default_factory=list)

    def _prune(self, pipeline: str, window: timedelta, now: datetime) -> None:
        cutoff = now - window
        self._entries = [
            e for e in self._entries
            if not (e.pipeline == pipeline and e.timestamp < cutoff)
        ]

    def count_since(self, pipeline: str, since: datetime) -> int:
        return sum(1 for e in self._entries if e.pipeline == pipeline and e.timestamp >= since)

    def apply(self, alerts: List[Alert], config: QuotaConfig, now: datetime | None = None) -> QuotaResult:
        if now is None:
            now = datetime.utcnow()
        self._prune(config.pipeline, config.window, now)
        result = QuotaResult()
        window_start = now - config.window
        for alert in alerts:
            if alert.pipeline != config.pipeline:
                result.kept.append(alert)
                continue
            current = self.count_since(config.pipeline, window_start)
            if current < config.max_alerts:
                self._entries.append(QuotaEntry(pipeline=alert.pipeline, timestamp=now))
                result.kept.append(alert)
            else:
                result.dropped.append(alert)
        return result
