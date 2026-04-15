"""Exponential backoff tracking for repeated alert failures."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional


@dataclass
class BackoffConfig:
    base_delay_seconds: int = 60
    max_delay_seconds: int = 3600
    multiplier: float = 2.0
    max_attempts: int = 8

    @property
    def base_delay(self) -> timedelta:
        return timedelta(seconds=self.base_delay_seconds)

    @property
    def max_delay(self) -> timedelta:
        return timedelta(seconds=self.max_delay_seconds)


@dataclass
class BackoffEntry:
    pipeline: str
    rule_name: str
    attempts: int = 0
    last_fired: Optional[datetime] = None
    next_allowed: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "rule_name": self.rule_name,
            "attempts": self.attempts,
            "last_fired": self.last_fired.isoformat() if self.last_fired else None,
            "next_allowed": self.next_allowed.isoformat() if self.next_allowed else None,
        }


@dataclass
class BackoffResult:
    alert_pipeline: str
    alert_rule: str
    allowed: bool
    attempts: int
    next_allowed: Optional[datetime]

    def to_dict(self) -> dict:
        return {
            "pipeline": self.alert_pipeline,
            "rule": self.alert_rule,
            "allowed": self.allowed,
            "attempts": self.attempts,
            "next_allowed": self.next_allowed.isoformat() if self.next_allowed else None,
        }


class AlertBackoffManager:
    def __init__(self, config: BackoffConfig) -> None:
        self._config = config
        self._entries: Dict[str, BackoffEntry] = {}

    def _key(self, pipeline: str, rule_name: str) -> str:
        return f"{pipeline}::{rule_name}"

    def _compute_delay(self, attempts: int) -> timedelta:
        delay_seconds = self._config.base_delay_seconds * (
            self._config.multiplier ** max(0, attempts - 1)
        )
        capped = min(delay_seconds, self._config.max_delay_seconds)
        return timedelta(seconds=capped)

    def check(self, pipeline: str, rule_name: str, now: Optional[datetime] = None) -> BackoffResult:
        now = now or datetime.utcnow()
        key = self._key(pipeline, rule_name)
        entry = self._entries.get(key)

        if entry is None or entry.next_allowed is None or now >= entry.next_allowed:
            attempts = (entry.attempts if entry else 0) + 1
            delay = self._compute_delay(attempts)
            next_allowed = now + delay
            self._entries[key] = BackoffEntry(
                pipeline=pipeline,
                rule_name=rule_name,
                attempts=attempts,
                last_fired=now,
                next_allowed=next_allowed,
            )
            return BackoffResult(
                alert_pipeline=pipeline,
                alert_rule=rule_name,
                allowed=True,
                attempts=attempts,
                next_allowed=next_allowed,
            )

        return BackoffResult(
            alert_pipeline=pipeline,
            alert_rule=rule_name,
            allowed=False,
            attempts=entry.attempts,
            next_allowed=entry.next_allowed,
        )

    def reset(self, pipeline: str, rule_name: str) -> None:
        key = self._key(pipeline, rule_name)
        self._entries.pop(key, None)

    def entries(self) -> List[BackoffEntry]:
        return list(self._entries.values())
