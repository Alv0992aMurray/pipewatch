"""Pipeline health reporter: formats and outputs metric snapshots and alert summaries."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.metrics import PipelineMetric, is_healthy, to_dict


def _severity_label(severity: AlertSeverity) -> str:
    labels = {
        AlertSeverity.INFO: "INFO",
        AlertSeverity.WARNING: "WARNING",
        AlertSeverity.CRITICAL: "CRITICAL",
    }
    return labels.get(severity, "UNKNOWN")


def format_metric_summary(metric: PipelineMetric) -> str:
    """Return a human-readable single-line summary of a pipeline metric."""
    healthy = "HEALTHY" if is_healthy(metric) else "UNHEALTHY"
    return (
        f"[{healthy}] pipeline={metric.pipeline_name} "
        f"rows_in={metric.rows_in} rows_out={metric.rows_out} "
        f"errors={metric.error_count} duration={metric.duration_seconds:.2f}s"
    )


def format_alerts(alerts: List[Alert]) -> str:
    """Return a formatted multi-line string listing all triggered alerts."""
    if not alerts:
        return "No alerts triggered."
    lines = []
    for alert in alerts:
        label = _severity_label(alert.severity)
        lines.append(f"  [{label}] {alert.rule_name}: {alert.message}")
    return "\n".join(lines)


def build_report(
    metric: PipelineMetric,
    alerts: List[Alert],
    timestamp: Optional[datetime] = None,
) -> dict:
    """Build a structured report dict combining metric data and alert results."""
    ts = timestamp or datetime.now(timezone.utc)
    return {
        "timestamp": ts.isoformat(),
        "metric": to_dict(metric),
        "healthy": is_healthy(metric),
        "alerts": [
            {
                "rule": a.rule_name,
                "severity": _severity_label(a.severity),
                "message": a.message,
            }
            for a in alerts
        ],
        "alert_count": len(alerts),
    }


def report_to_json(report: dict, indent: int = 2) -> str:
    """Serialize a report dict to a JSON string."""
    return json.dumps(report, indent=indent)
