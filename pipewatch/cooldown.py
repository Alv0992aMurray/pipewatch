"""Cooldown tracking: prevent re-alerting on a pipeline until a quiet period elapses."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pipewatch.alerts import Alert


@dataclass
class CooldownConfig:
    pipeline: str
    seconds: int

    @property
    def window(self) -> timedelta:
        return timedelta(seconds=self.seconds)


@dataclass
class CooldownEntry:
    pipeline: str
    last_alert_at: datetime
    cooldown_seconds: int

    def is_cooling(self, now: Optional[datetime] = None) -> bool:
        now = now or datetime.utcnow()
        return (now - self.last_alert_at) < timedelta(seconds=self.cooldown_seconds)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "last_alert_at": self.last_alert_at.isoformat(),
            "cooldown_seconds": self.cooldown_seconds,
        }


@dataclass
class CooldownResult:
    kept: List[Alert]
    suppressed: List[Alert]

    @property
    def total_suppressed(self) -> int:
        return len(self.suppressed)


@dataclass
class CooldownManager:
    configs: List[CooldownConfig]
    _entries: Dict[str, CooldownEntry] = field(default_factory=dict)

    def _config_for(self, pipeline: str) -> Optional[CooldownConfig]:
        for cfg in self.configs:
            if cfg.pipeline == pipeline or cfg.pipeline == "*":
                return cfg
        return None

    def apply(self, alerts: List[Alert], now: Optional[datetime] = None) -> CooldownResult:
        now = now or datetime.utcnow()
        kept: List[Alert] = []
        suppressed: List[Alert] = []

        for alert in alerts:
            cfg = self._config_for(alert.pipeline)
            if cfg is None:
                kept.append(alert)
                continue

            entry = self._entries.get(alert.pipeline)
            if entry and entry.is_cooling(now):
                suppressed.append(alert)
            else:
                kept.append(alert)
                self._entries[alert.pipeline] = CooldownEntry(
                    pipeline=alert.pipeline,
                    last_alert_at=now,
                    cooldown_seconds=cfg.seconds,
                )

        return CooldownResult(kept=kept, suppressed=suppressed)

    def entries(self) -> List[CooldownEntry]:
        return list(self._entries.values())
