"""Human-readable and JSON formatting for ForecastResult objects."""
from __future__ import annotations

import json
from typing import List

from pipewatch.forecast import ForecastResult


def _trend_arrow(slope: float | None) -> str:
    if slope is None:
        return "?"
    if slope > 0.001:
        return "↑"
    if slope < -0.001:
        return "↓"
    return "→"


def format_forecast_result(result: ForecastResult) -> str:
    arrow = _trend_arrow(result.slope)
    if result.predicted_rate is None:
        return (
            f"  [{result.pipeline}] forecast unavailable — {result.message}"
        )
    return (
        f"  [{result.pipeline}] {arrow} "
        f"predicted={result.predicted_rate:.1%}  "
        f"slope={result.slope:+.4f}  "
        f"horizon={result.horizon}  "
        f"pts={result.data_points}"
    )


def format_forecast_report(results: List[ForecastResult]) -> str:
    if not results:
        return "Forecast Report\n  (no pipelines)"
    lines = ["Forecast Report"]
    for r in results:
        lines.append(format_forecast_result(r))
    return "\n".join(lines)


def forecast_report_to_json(results: List[ForecastResult]) -> str:
    return json.dumps([r.to_dict() for r in results], indent=2)
