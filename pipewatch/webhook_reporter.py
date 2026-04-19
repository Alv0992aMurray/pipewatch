"""Format webhook delivery results for CLI output."""
from __future__ import annotations

import json
from typing import List

from pipewatch.webhook import WebhookResult


def format_webhook_result(result: WebhookResult) -> str:
    """Format a single WebhookResult as a human-readable status line."""
    if result.success:
        code_str = f" (HTTP {result.status_code})" if result.status_code else ""
        return f"  [OK]  {result.url}{code_str}"
    err_str = f": {result.error}" if result.error else ""
    code_str = f" (HTTP {result.status_code})" if result.status_code else ""
    return f"  [FAIL] {result.url}{code_str}{err_str}"


def format_webhook_report(results: List[WebhookResult]) -> str:
    """Format a list of WebhookResults as a multi-line summary report."""
    if not results:
        return "Webhooks: none configured or no alerts to send."
    lines = ["Webhook Notifications:"]
    lines.extend(format_webhook_result(r) for r in results)
    ok = sum(1 for r in results if r.success)
    lines.append(f"  {ok}/{len(results)} delivered successfully.")
    return "\n".join(lines)


def webhook_report_to_json(results: List[WebhookResult]) -> str:
    """Serialize a list of WebhookResults to a JSON string."""
    data = [
        {
            "url": r.url,
            "success": r.success,
            "status_code": r.status_code,
            "error": r.error,
        }
        for r in results
    ]
    return json.dumps({"webhook_results": data}, indent=2)


def webhook_summary(results: List[WebhookResult]) -> dict:
    """Return a summary dict with counts of successes and failures.

    Example::

        {"total": 3, "succeeded": 2, "failed": 1}
    """
    succeeded = sum(1 for r in results if r.success)
    return {
        "total": len(results),
        "succeeded": succeeded,
        "failed": len(results) - succeeded,
    }
