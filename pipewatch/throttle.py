"""Alert throttling: suppress repeated alerts for a pipeline within a time window."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pipewatch.alerts import Alert, AlertSeverity


@dataclass
class ThrottleConfig:
    """Configuration for alert throttling."""
    pipeline: str
    window_seconds: int = 300  # 5 minutes default
    max_alerts: int = 3

    @property
    def window(self) -> timedelta:
        return timedelta(seconds=self.window_seconds)


@dataclass
class ThrottleEntry:
    """Tracks alert timestamps for a single pipeline."""
    pipeline: str
    timestamps: List[datetime] = field(default_factory=list)

    def prune(self, window: timedelta, now: datetime) -> None:
        cutoff = now - window
        self.timestamps = [t for t in self.timestamps if t >= cutoff]

    def count(self) -> int:
        return len(self.timestamps)

    def record(self, now: datetime) -> None:
        self.timestamps.append(now)


@dataclass
class ThrottleResult:
    """Result of applying throttle logic to a list of alerts."""
    kept: List[Alert]
    suppressed: List[Alert]

    @property
    def total_suppressed(self) -> int:
        return len(self.suppressed)

    def to_dict(self) -> dict:
        return {
            "kept": [a.to_dict() for a in self.kept],
            "suppressed": [a.to_dict() for a in self.suppressed],
            "total_suppressed": self.total_suppressed,
        }


class AlertThrottler:
    """Throttles alerts per pipeline based on a sliding window."""

    def __init__(self, configs: List[ThrottleConfig]) -> None:
        self._configs: Dict[str, ThrottleConfig] = {c.pipeline: c for c in configs}
        self._entries: Dict[str, ThrottleEntry] = {}

    def _get_config(self, pipeline: str) -> Optional[ThrottleConfig]:
        return self._configs.get(pipeline) or self._configs.get("*")

    def _get_entry(self, pipeline: str) -> ThrottleEntry:
        if pipeline not in self._entries:
            self._entries[pipeline] = ThrottleEntry(pipeline=pipeline)
        return self._entries[pipeline]

    def apply(self, alerts: List[Alert], now: Optional[datetime] = None) -> ThrottleResult:
        if now is None:
            now = datetime.utcnow()

        kept: List[Alert] = []
        suppressed: List[Alert] = []

        for alert in alerts:
            config = self._get_config(alert.pipeline)
            if config is None:
                kept.append(alert)
                continue

            entry = self._get_entry(alert.pipeline)
            entry.prune(config.window, now)

            if entry.count() < config.max_alerts:
                entry.record(now)
                kept.append(alert)
            else:
                suppressed.append(alert)

        return ThrottleResult(kept=kept, suppressed=suppressed)
