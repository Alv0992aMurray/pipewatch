"""Signal aggregation: combine multiple alert indicators into a single health signal score."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.alerts import Alert, AlertSeverity


@dataclass
class SignalResult:
    pipeline: str
    score: float  # 0.0 (critical) to 1.0 (healthy)
    grade: str
    alert_count: int
    critical_count: int
    warning_count: int
    dominant_severity: Optional[str]

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "score": round(self.score, 3),
            "grade": self.grade,
            "alert_count": self.alert_count,
            "critical_count": self.critical_count,
            "warning_count": self.warning_count,
            "dominant_severity": self.dominant_severity,
        }


def _grade(score: float) -> str:
    if score >= 0.9:
        return "A"
    if score >= 0.75:
        return "B"
    if score >= 0.5:
        return "C"
    if score >= 0.25:
        return "D"
    return "F"


def compute_signal(pipeline: str, alerts: List[Alert], base_score: float = 1.0) -> SignalResult:
    """Compute an aggregated health signal from a list of alerts.

    Each warning deducts 0.15; each critical deducts 0.35.
    Score is clamped to [0.0, 1.0].
    """
    critical_count = sum(1 for a in alerts if a.severity == AlertSeverity.CRITICAL)
    warning_count = sum(1 for a in alerts if a.severity == AlertSeverity.WARNING)

    penalty = (critical_count * 0.35) + (warning_count * 0.15)
    score = max(0.0, min(1.0, base_score - penalty))

    if critical_count > 0:
        dominant = AlertSeverity.CRITICAL.value
    elif warning_count > 0:
        dominant = AlertSeverity.WARNING.value
    else:
        dominant = None

    return SignalResult(
        pipeline=pipeline,
        score=score,
        grade=_grade(score),
        alert_count=len(alerts),
        critical_count=critical_count,
        warning_count=warning_count,
        dominant_severity=dominant,
    )
