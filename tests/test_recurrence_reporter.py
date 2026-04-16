"""Tests for pipewatch.recurrence_reporter."""
import json
from datetime import datetime

from pipewatch.alerts import Alert, AlertSeverity
from pipewatch.recurrence import RecurrenceEntry, RecurrenceResult, RecurrenceTracker
from pipewatch.recurrence_reporter import (
    format_recurrence_entry,
    format_recurrence_report,
    recurrence_report_to_json,
)


def _ts(n: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, 0, n)


def _entry(occurrences: int = 1) -> RecurrenceEntry:
    return RecurrenceEntry(
        pipeline="pipe_a",
        rule_name="low_success",
        occurrences=occurrences,
        first_seen=_ts(0),
        last_seen=_ts(60),
    )


def test_format_entry_single_occurrence_no_tag():
    text = format_recurrence_entry(_entry(occurrences=1))
    assert "[RECURRING]" not in text
    assert "pipe_a" in text
    assert "low_success" in text


def test_format_entry_multiple_occurrences_shows_tag():
    text = format_recurrence_entry(_entry(occurrences=3))
    assert "[RECURRING]" in text
    assert "3" in text


def test_format_report_empty_result():
    result = RecurrenceResult(entries=[])
    text = format_recurrence_report(result)
    assert "No recurrence data" in text


def test_format_report_shows_total_recurring():
    result = RecurrenceResult(entries=[_entry(occurrences=2), _entry(occurrences=1)])
    text = format_recurrence_report(result)
    assert "Total recurring" in text
    assert "1" in text


def test_format_report_sorted_by_occurrences_descending():
    entries = [
        RecurrenceEntry("pipe_b", "rule_x", 1, _ts(0), _ts(10)),
        RecurrenceEntry("pipe_a", "rule_y", 5, _ts(0), _ts(10)),
    ]
    result = RecurrenceResult(entries=entries)
    text = format_recurrence_report(result)
    pos_a = text.index("pipe_a")
    pos_b = text.index("pipe_b")
    assert pos_a < pos_b


def test_json_output_is_valid():
    result = RecurrenceResult(entries=[_entry(occurrences=2)])
    raw = recurrence_report_to_json(result)
    parsed = json.loads(raw)
    assert "total_recurring" in parsed
    assert "entries" in parsed
    assert len(parsed["entries"]) == 1


def test_json_entry_has_iso_timestamps():
    result = RecurrenceResult(entries=[_entry(occurrences=1)])
    parsed = json.loads(recurrence_report_to_json(result))
    entry = parsed["entries"][0]
    assert "T" in entry["first_seen"]
    assert "T" in entry["last_seen"]
