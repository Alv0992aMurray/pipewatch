"""Format webhook delivery results for CLI output."""
from __future__ import annotations

import json
from typing import List

from pipewatch.webhook import WebhookResult


def format_webhook_result(result: WebhookResult) -> str:
    if result.success:
        code_str = f" (HTTP {result.status_code})" if result.status_code else ""
        return f"  [OK]  {result.url}{code_str}"
    err_str = f": {result.error}" if result.error else ""
    code_str = f" (HTTP {result.status_code})" if result.status_code else ""
    return f"  [FAIL] {result.url}{code_str}{err_str}"


def format_webhook_report(results: List[WebhookResult]) -> str:
    if not results:
        return "Webhooks: none configured or no alerts to send."
    lines = ["Webhook Notifications:"]
    lines.extend(format_webhook_result(r) for r in results)
    ok = sum(1 for r in results if r.success)
    lines.append(f"  {ok}/{len(results)} delivered successfully.")
    return "\n".join(lines)


def webhook_report_to_json(results: List[WebhookResult]) -> str:
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
