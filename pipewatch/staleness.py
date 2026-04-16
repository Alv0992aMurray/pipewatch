"""Staleness detection: flags pipelines that haven't reported metrics recently."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from pipewatch.history import PipelineHistory


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class StalenessConfig:
    pipeline: str
    max_age_seconds: int

    @property
    def max_age(self) -> timedelta:
        return timedelta(seconds=self.max_age_seconds)


@dataclass
class StalenessResult:
    pipeline: str
    last_seen: Optional[datetime]
    age_seconds: Optional[float]
    is_stale: bool
    threshold_seconds: int

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "last_seen": self.last_seen.isoformat() if self.last_seen else None,
            "age_seconds": round(self.age_seconds, 1) if self.age_seconds is not None else None,
            "is_stale": self.is_stale,
            "threshold_seconds": self.threshold_seconds,
        }


def check_staleness(
    config: StalenessConfig,
    history: PipelineHistory,
    now: Optional[datetime] = None,
) -> StalenessResult:
    """Return a StalenessResult for the given pipeline history."""
    if now is None:
        now = _now()

    snapshots = history.last_n(1)
    if not snapshots:
        return StalenessResult(
            pipeline=config.pipeline,
            last_seen=None,
            age_seconds=None,
            is_stale=True,
            threshold_seconds=config.max_age_seconds,
        )

    last_snap = snapshots[-1]
    last_seen = last_snap.timestamp
    if last_seen.tzinfo is None:
        last_seen = last_seen.replace(tzinfo=timezone.utc)

    age = (now - last_seen).total_seconds()
    is_stale = age > config.max_age_seconds

    return StalenessResult(
        pipeline=config.pipeline,
        last_seen=last_seen,
        age_seconds=age,
        is_stale=is_stale,
        threshold_seconds=config.max_age_seconds,
    )


def check_all_staleness(
    configs: list[StalenessConfig],
    histories: dict[str, PipelineHistory],
    now: Optional[datetime] = None,
) -> list[StalenessResult]:
    """Check staleness for multiple pipelines."""
    results = []
    for cfg in configs:
        hist = histories.get(cfg.pipeline, PipelineHistory(pipeline=cfg.pipeline))
        results.append(check_staleness(cfg, hist, now=now))
    return results
