"""Formatting helpers for health score reports."""
from __future__ import annotations
import json
from pipewatch.health_score import HealthScoreReport, PipelineHealthScore


def _bar(score: float, width: int = 20) -> str:
    filled = int(round(score / 100 * width))
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def format_pipeline_score(entry: PipelineHealthScore) -> str:
    status = "OK" if entry.is_healthy else "FAIL"
    bar = _bar(entry.score)
    return (
        f"  {entry.pipeline:<30} {bar} {entry.score:5.1f}  "
        f"Grade:{entry.grade}  SR:{entry.success_rate:.1%}  "
        f"TP:{entry.throughput:.1f}/s  [{status}]"
    )


def format_health_score_report(report: HealthScoreReport) -> str:
    if not report.scores:
        return "Health Score Report: no pipelines."
    lines = ["=== Health Score Report ==="]
    for entry in report.scores:
        lines.append(format_pipeline_score(entry))
    avg = report.average_score
    lines.append("---")
    lines.append(
        f"  Pipelines: {len(report.scores)}  "
        f"Healthy: {report.healthy_count}  "
        f"Unhealthy: {report.unhealthy_count}  "
        f"Avg Score: {avg:.1f}" if avg is not None else "  Avg Score: n/a"
    )
    return "\n".join(lines)


def health_score_report_to_json(report: HealthScoreReport) -> str:
    avg = report.average_score
    payload = {
        "pipelines": [s.to_dict() for s in report.scores],
        "summary": {
            "total": len(report.scores),
            "healthy": report.healthy_count,
            "unhealthy": report.unhealthy_count,
            "average_score": round(avg, 2) if avg is not None else None,
        },
    }
    return json.dumps(payload, indent=2)
