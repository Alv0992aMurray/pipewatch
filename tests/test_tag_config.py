"""Tests for pipewatch.tag_config."""
from __future__ import annotations

import textwrap

import pytest

from pipewatch.tag_config import TagConfigError, load_tag_filters


@pytest.fixture()
def write_yaml(tmp_path):
    def _write(content: str) -> str:
        p = tmp_path / "tags.yml"
        p.write_text(textwrap.dedent(content))
        return str(p)
    return _write


def test_load_basic_tag_filters(write_yaml):
    path = write_yaml("""
        tag_filters:
          - required:
              env: prod
              team: data
    """)
    filters = load_tag_filters(path)
    assert len(filters) == 1
    assert filters[0].required == {"env": "prod", "team": "data"}


def test_load_multiple_filters(write_yaml):
    path = write_yaml("""
        tag_filters:
          - required:
              env: prod
          - required:
              env: staging
    """)
    filters = load_tag_filters(path)
    assert len(filters) == 2
    assert filters[1].required == {"env": "staging"}


def test_empty_file_returns_empty_list(write_yaml):
    path = write_yaml("")
    assert load_tag_filters(path) == []


def test_missing_tag_filters_key_returns_empty_list(write_yaml):
    path = write_yaml("other_key: value\n")
    assert load_tag_filters(path) == []


def test_missing_file_raises_error(tmp_path):
    with pytest.raises(TagConfigError, match="not found"):
        load_tag_filters(str(tmp_path / "nonexistent.yml"))


def test_invalid_filter_entry_raises_error(write_yaml):
    path = write_yaml("""
        tag_filters:
          - just_a_string
    """)
    with pytest.raises(TagConfigError, match="must be a mapping"):
        load_tag_filters(path)


def test_invalid_required_type_raises_error(write_yaml):
    path = write_yaml("""
        tag_filters:
          - required:
              - env
              - prod
    """)
    with pytest.raises(TagConfigError, match="key/value mapping"):
        load_tag_filters(path)
