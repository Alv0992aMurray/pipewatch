"""Human-readable and JSON reporters for VolatilityResult."""
from __future__ import annotations

import json
from typing import Optional

from pipewatch.volatility import VolatilityResult


def _flag(result: VolatilityResult) -> str:
    if result.insufficient_data:
        return "⏳"
    return "🔥" if result.is_volatile else "✅"


def format_volatility_result(result: VolatilityResult) -> str:
    icon = _flag(result)
    if result.insufficient_data:
        return (
            f"{icon} {result.pipeline} [{result.metric}] — "
            f"insufficient data ({result.sample_count} sample(s))"
        )
    label = "VOLATILE" if result.is_volatile else "stable"
    return (
        f"{icon} {result.pipeline} [{result.metric}] — {label} "
        f"(CV={result.coefficient_of_variation:.3f}, "
        f"mean={result.mean:.3f}, σ={result.std_dev:.3f}, "
        f"threshold={result.threshold:.3f})"
    )


def format_volatility_report(results: list[VolatilityResult]) -> str:
    if not results:
        return "No volatility results to display."
    lines = ["=== Volatility Report ==="]
    for r in results:
        lines.append(format_volatility_result(r))
    return "\n".join(lines)


def volatility_report_to_json(results: list[VolatilityResult]) -> str:
    return json.dumps([r.to_dict() for r in results], indent=2)
