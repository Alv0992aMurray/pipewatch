"""Human-readable and JSON reporting for RegressionResult."""
from __future__ import annotations

import json
from typing import Optional

from pipewatch.regression import RegressionResult


def _pct(value: float) -> str:
    sign = "+" if value >= 0 else ""
    return f"{sign}{value * 100:.1f}%"


def format_regression_result(result: Optional[RegressionResult]) -> str:
    if result is None:
        return "  [regression] insufficient data"

    status = "REGRESSION" if result.regressed else "OK"
    icon = "\u26a0\ufe0f" if result.regressed else "\u2705"
    lines = [
        f"  {icon} [{status}] {result.pipeline} / {result.metric}",
        f"     current : {result.current_value:.4f}",
        f"     baseline: {result.baseline_mean:.4f}  (n={result.sample_size})",
        f"     change  : {_pct(result.pct_change)}  (threshold: -{result.threshold_pct * 100:.0f}%)",
    ]
    return "\n".join(lines)


def format_regression_report(results: list[Optional[RegressionResult]]) -> str:
    if not results:
        return "No regression checks performed."
    header = "=== Regression Report ==="
    body = "\n".join(format_regression_result(r) for r in results)
    return f"{header}\n{body}"


def regression_report_to_json(results: list[Optional[RegressionResult]]) -> str:
    payload = [r.to_dict() if r is not None else None for r in results]
    return json.dumps(payload, indent=2)
