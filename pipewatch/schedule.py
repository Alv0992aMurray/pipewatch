"""Schedule-based pipeline run tracking for pipewatch."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional


class ScheduleStatus(str, Enum):
    ON_TIME = "on_time"
    LATE = "late"
    MISSING = "missing"
    UNKNOWN = "unknown"


@dataclass
class ScheduleConfig:
    """Defines the expected run cadence for a pipeline."""
    pipeline_name: str
    interval_minutes: int
    grace_period_minutes: int = 5

    @property
    def interval(self) -> timedelta:
        return timedelta(minutes=self.interval_minutes)

    @property
    def grace_period(self) -> timedelta:
        return timedelta(minutes=self.grace_period_minutes)


@dataclass
class ScheduleCheck:
    """Result of checking a pipeline against its schedule."""
    pipeline_name: str
    status: ScheduleStatus
    last_run: Optional[datetime]
    expected_by: Optional[datetime]
    checked_at: datetime = field(default_factory=datetime.utcnow)

    def is_overdue(self) -> bool:
        return self.status in (ScheduleStatus.LATE, ScheduleStatus.MISSING)

    def to_dict(self) -> dict:
        return {
            "pipeline_name": self.pipeline_name,
            "status": self.status.value,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "expected_by": self.expected_by.isoformat() if self.expected_by else None,
            "checked_at": self.checked_at.isoformat(),
        }


def check_schedule(
    config: ScheduleConfig,
    last_run: Optional[datetime],
    now: Optional[datetime] = None,
) -> ScheduleCheck:
    """Evaluate whether a pipeline has run on schedule."""
    if now is None:
        now = datetime.utcnow()

    if last_run is None:
        return ScheduleCheck(
            pipeline_name=config.pipeline_name,
            status=ScheduleStatus.UNKNOWN,
            last_run=None,
            expected_by=None,
            checked_at=now,
        )

    expected_by = last_run + config.interval
    deadline = expected_by + config.grace_period

    if now <= deadline:
        status = ScheduleStatus.ON_TIME
    elif now <= expected_by + config.interval:
        status = ScheduleStatus.LATE
    else:
        status = ScheduleStatus.MISSING

    return ScheduleCheck(
        pipeline_name=config.pipeline_name,
        status=status,
        last_run=last_run,
        expected_by=expected_by,
        checked_at=now,
    )
