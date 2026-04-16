"""Reporter for masking results."""
from __future__ import annotations
import json
from pipewatch.masking import MaskResult


def format_mask_result(result: MaskResult) -> str:
    lines = []
    if not result.fields_masked:
        lines.append("  No sensitive fields detected.")
    else:
        lines.append(f"  Masked {len(result.fields_masked)} field(s):")
        for f in result.fields_masked:
            lines.append(f"    - {f}")
    return "\n".join(lines)


def format_masking_report(results: list[MaskResult]) -> str:
    if not results:
        return "Masking Report\n  No records processed."
    lines = ["Masking Report"]
    total = sum(len(r.fields_masked) for r in results)
    lines.append(f"  Records processed : {len(results)}")
    lines.append(f"  Total fields masked: {total}")
    for i, r in enumerate(results, 1):
        lines.append(f"  Record {i}:")
        lines.append(format_mask_result(r))
    return "\n".join(lines)


def masking_report_to_json(results: list[MaskResult]) -> str:
    return json.dumps([r.to_dict() for r in results], indent=2)
