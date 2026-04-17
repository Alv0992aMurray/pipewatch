"""Heartbeat tracking: detect pipelines that have stopped reporting."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from pipewatch.history import PipelineHistory


def _now() -> datetime:
    return datetime.utcnow()


@dataclass
class HeartbeatConfig:
    pipeline: str
    expected_interval_seconds: int = 300  # 5 minutes
    grace_seconds: int = 60

    @property
    def deadline(self) -> timedelta:
        return timedelta(seconds=self.expected_interval_seconds + self.grace_seconds)


@dataclass
class HeartbeatResult:
    pipeline: str
    last_seen: Optional[datetime]
    expected_interval_seconds: int
    grace_seconds: int
    missed: bool
    seconds_since_last: Optional[float]

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "last_seen": self.last_seen.isoformat() if self.last_seen else None,
            "expected_interval_seconds": self.expected_interval_seconds,
            "grace_seconds": self.grace_seconds,
            "missed": self.missed,
            "seconds_since_last": round(self.seconds_since_last, 1) if self.seconds_since_last is not None else None,
        }


def check_heartbeat(
    config: HeartbeatConfig,
    history: PipelineHistory,
    now: Optional[datetime] = None,
) -> HeartbeatResult:
    if now is None:
        now = _now()

    snapshots = history.last_n(1)
    if not snapshots:
        return HeartbeatResult(
            pipeline=config.pipeline,
            last_seen=None,
            expected_interval_seconds=config.expected_interval_seconds,
            grace_seconds=config.grace_seconds,
            missed=True,
            seconds_since_last=None,
        )

    last_seen = snapshots[0].timestamp
    elapsed = (now - last_seen).total_seconds()
    deadline = config.expected_interval_seconds + config.grace_seconds
    missed = elapsed > deadline

    return HeartbeatResult(
        pipeline=config.pipeline,
        last_seen=last_seen,
        expected_interval_seconds=config.expected_interval_seconds,
        grace_seconds=config.grace_seconds,
        missed=missed,
        seconds_since_last=elapsed,
    )
