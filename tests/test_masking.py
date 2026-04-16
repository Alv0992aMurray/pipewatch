"""Tests for pipewatch.masking and pipewatch.masking_reporter."""
import pytest
from pipewatch.masking import MaskingConfig, mask_dict, apply_masking, SENSITIVE_KEYS
from pipewatch.masking_reporter import format_mask_result, format_masking_report, masking_report_to_json


def test_default_config_masks_known_keys():
    data = {"username": "alice", "password": "s3cr3t"}
    result = mask_dict(data)
    assert result.masked["password"] == "***"
    assert result.masked["username"] == "alice"
    assert "password" in result.fields_masked


def test_no_sensitive_keys_returns_empty_masked_list():
    data = {"pipeline": "etl", "rows": 100}
    result = mask_dict(data)
    assert result.fields_masked == []
    assert result.masked == data


def test_custom_mask_string():
    config = MaskingConfig(mask="[REDACTED]")
    data = {"token": "abc123"}
    result = mask_dict(data, config)
    assert result.masked["token"] == "[REDACTED]"


def test_custom_keys():
    config = MaskingConfig(keys={"internal_id"})
    data = {"internal_id": "42", "name": "pipe"}
    result = mask_dict(data, config)
    assert result.masked["internal_id"] == "***"
    assert result.masked["name"] == "pipe"


def test_nested_dict_masked():
    data = {"meta": {"api_key": "xyz", "owner": "bob"}}
    result = mask_dict(data)
    assert result.masked["meta"]["api_key"] == "***"
    assert result.masked["meta"]["owner"] == "bob"
    assert "meta.api_key" in result.fields_masked


def test_case_insensitive_by_default():
    data = {"API_KEY": "secret"}
    result = mask_dict(data)
    assert result.masked["API_KEY"] == "***"


def test_case_sensitive_does_not_mask_wrong_case():
    config = MaskingConfig(keys={"api_key"}, case_sensitive=True)
    data = {"API_KEY": "secret"}
    result = mask_dict(data, config)
    assert result.masked["API_KEY"] == "secret"
    assert result.fields_masked == []


def test_apply_masking_returns_list_of_masked_dicts():
    records = [
        {"token": "t1", "rows": 10},
        {"token": "t2", "rows": 20},
    ]
    masked = apply_masking(records)
    assert all(r["token"] == "***" for r in masked)
    assert masked[0]["rows"] == 10


def test_format_mask_result_no_fields():
    data = {"rows": 5}
    result = mask_dict(data)
    out = format_mask_result(result)
    assert "No sensitive" in out


def test_format_mask_result_with_fields():
    data = {"secret": "x"}
    result = mask_dict(data)
    out = format_mask_result(result)
    assert "secret" in out
    assert "1 field" in out


def test_format_masking_report_empty():
    out = format_masking_report([])
    assert "No records" in out


def test_format_masking_report_counts():
    records = [{"password": "p", "name": "n"}, {"api_key": "k"}]
    results = [mask_dict(r) for r in records]
    out = format_masking_report(results)
    assert "2" in out
    assert "Total fields masked" in out


def test_masking_report_to_json():
    import json
    data = {"token": "abc"}
    results = [mask_dict(data)]
    out = json.loads(masking_report_to_json(results))
    assert out[0]["count"] == 1
