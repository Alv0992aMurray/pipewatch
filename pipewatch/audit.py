"""Audit log for pipeline check runs — records what ran, when, and what was found."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.alerts import Alert
from pipewatch.metrics import PipelineMetric


@dataclass
class AuditEntry:
    pipeline: str
    checked_at: datetime
    total_rows: int
    failed_rows: int
    success_rate: float
    is_healthy: bool
    alert_count: int
    alert_summaries: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "checked_at": self.checked_at.isoformat(),
            "total_rows": self.total_rows,
            "failed_rows": self.failed_rows,
            "success_rate": round(self.success_rate, 4),
            "is_healthy": self.is_healthy,
            "alert_count": self.alert_count,
            "alert_summaries": self.alert_summaries,
        }


@dataclass
class AuditLog:
    entries: List[AuditEntry] = field(default_factory=list)

    def record(self, entry: AuditEntry) -> None:
        self.entries.append(entry)

    def last_n(self, n: int) -> List[AuditEntry]:
        return self.entries[-n:] if n > 0 else []

    def entries_for(self, pipeline: str) -> List[AuditEntry]:
        return [e for e in self.entries if e.pipeline == pipeline]

    def total_runs(self) -> int:
        return len(self.entries)

    def unhealthy_runs(self) -> int:
        return sum(1 for e in self.entries if not e.is_healthy)


def build_audit_entry(
    metric: PipelineMetric,
    alerts: List[Alert],
    checked_at: Optional[datetime] = None,
) -> AuditEntry:
    from pipewatch.metrics import success_rate as calc_rate, is_healthy as check_health

    ts = checked_at or datetime.now(timezone.utc)
    rate = calc_rate(metric)
    healthy = check_health(metric)
    summaries = [a.message for a in alerts]

    return AuditEntry(
        pipeline=metric.pipeline_name,
        checked_at=ts,
        total_rows=metric.total_rows,
        failed_rows=metric.failed_rows,
        success_rate=rate,
        is_healthy=healthy,
        alert_count=len(alerts),
        alert_summaries=summaries,
    )


def audit_log_to_jsonl(log: AuditLog) -> str:
    lines = [json.dumps(e.to_dict()) for e in log.entries]
    return "\n".join(lines)
