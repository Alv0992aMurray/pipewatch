"""Digest: generate a periodic summary report from pipeline history and trends."""

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.history import PipelineHistory
from pipewatch.trend import TrendReport, analyse_trend
from pipewatch.alerts import AlertRule, Alert, evaluate
from pipewatch.history import MetricSnapshot


@dataclass
class DigestEntry:
    pipeline_name: str
    snapshot_count: int
    latest_success_rate: Optional[float]
    latest_throughput: Optional[float]
    trend: TrendReport
    alerts: List[Alert]

    def has_alerts(self) -> bool:
        return len(self.alerts) > 0


@dataclass
class Digest:
    entries: List[DigestEntry]

    def pipelines_with_alerts(self) -> List[DigestEntry]:
        return [e for e in self.entries if e.has_alerts()]

    def total_alerts(self) -> int:
        return sum(len(e.alerts) for e in self.entries)


def _latest_snapshot(history: PipelineHistory) -> Optional[MetricSnapshot]:
    recent = history.last_n(1)
    return recent[0] if recent else None


def build_digest(
    histories: List[PipelineHistory],
    rules: List[AlertRule],
    window: int = 10,
) -> Digest:
    """Build a Digest from a list of PipelineHistory objects.

    Args:
        histories: One PipelineHistory per tracked pipeline.
        rules: Alert rules to evaluate against each latest snapshot.
        window: How many recent snapshots to include in trend analysis.

    Returns:
        A Digest summarising all pipelines.
    """
    entries: List[DigestEntry] = []

    for history in histories:
        trend = analyse_trend(history, n=window)
        latest = _latest_snapshot(history)

        alerts: List[Alert] = []
        if latest is not None:
            for rule in rules:
                alert = evaluate(rule, latest.to_metric())
                if alert is not None:
                    alerts.append(alert)

        entries.append(
            DigestEntry(
                pipeline_name=history.pipeline_name,
                snapshot_count=len(history.last_n(window)),
                latest_success_rate=latest.success_rate if latest else None,
                latest_throughput=latest.throughput if latest else None,
                trend=trend,
                alerts=alerts,
            )
        )

    return Digest(entries=entries)
