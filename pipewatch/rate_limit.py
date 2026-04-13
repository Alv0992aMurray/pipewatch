"""Rate limiting for alert notifications to prevent alert storms."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional


@dataclass
class RateLimitConfig:
    """Configuration for a rate limit window."""
    max_alerts: int
    window_seconds: int
    pipeline: str = "*"  # '*' means all pipelines

    @property
    def window(self) -> timedelta:
        return timedelta(seconds=self.window_seconds)


@dataclass
class R:
    """Tracks alert timestamps for a given pipeline."""
    pipeline: str
    timestamps: List[datetime] = field(default_factory=list)

    def prune(self, cutoff: datetime) -> None:
        """Remove timestamps older than the cutoff."""
        self.timestamps = [ts for ts in self.timestamps if ts >= cutoff]

    def count_since(self, cutoff: datetime) -> int:
        return sum(1 for ts in self.timestamps if ts >= cutoff)


@dataclass
class RateLimitResult:
    pipeline: str
    allowed: bool
    current_count: int
    max_alerts: int
    window_seconds: int

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "allowed": self.allowed,
            "current_count": self.current_count,
            "max_alerts": self.max_alerts,
            "window_seconds": self.window_seconds,
        }


class AlertRateLimiter:
    """Tracks per-pipeline alert rates and enforces limits."""

    def __init__(self, config: RateLimitConfig) -> None:
        self._config = config
        self._entries: Dict[str, RateLimitEntry] = {}

    def _get_entry(self, pipeline: str) -> RateLimitEntry:
        if pipeline not in self._entries:
            self._entries[pipeline] = RateLimitEntry(pipeline=pipeline)
        return self._entries[pipeline]

    def check(self, pipeline: str, now: Optional[datetime] = None) -> RateLimitResult:
        """Check whether an alert for *pipeline* is allowed under the rate limit."""
        if now is None:
            now = datetime.utcnow()

        # Only apply limit when config targets this pipeline or all pipelines
        if self._config.pipeline != "*" and self._config.pipeline != pipeline:
            return RateLimitResult(
                pipeline=pipeline,
                allowed=True,
                current_count=0,
                max_alerts=self._config.max_alerts,
                window_seconds=self._config.window_seconds,
            )

        entry = self._get_entry(pipeline)
        cutoff = now - self._config.window
        entry.prune(cutoff)
        count = entry.count_since(cutoff)
        allowed = count < self._config.max_alerts

        if allowed:
            entry.timestamps.append(now)

        return RateLimitResult(
            pipeline=pipeline,
            allowed=allowed,
            current_count=count,
            max_alerts=self._config.max_alerts,
            window_seconds=self._config.window_seconds,
        )

    def reset(self, pipeline: str) -> None:
        """Clear the rate limit history for a pipeline."""
        self._entries.pop(pipeline, None)
