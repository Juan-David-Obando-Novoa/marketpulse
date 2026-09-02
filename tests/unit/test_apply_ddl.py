"""The DDL splitter.

Small, but it decides whether table creation runs as six statements or as one
malformed blob, and the failure mode is a confusing Spark parse error rather
than anything that points here.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from marketpulse.streaming.apply_ddl import DEFAULT_DDL, split_statements

pytestmark = pytest.mark.unit


def test_splits_on_statement_boundaries() -> None:
    assert split_statements("select 1; select 2;") == ["select 1", "select 2"]


def test_trailing_semicolon_does_not_produce_an_empty_statement() -> None:
    """An empty statement is a Spark parse error, not a no-op."""
    assert split_statements("select 1;\n\n") == ["select 1"]


def test_missing_final_semicolon_is_tolerated() -> None:
    assert split_statements("select 1") == ["select 1"]


def test_semicolon_inside_a_string_literal_does_not_split() -> None:
    """The real bug this splitter had: the bronze DDL contains exactly this."""
    sql = "create table t (a int comment 'micro-batch time; differs from x');"
    assert split_statements(sql) == [
        "create table t (a int comment 'micro-batch time; differs from x')"
    ]


def test_escaped_quote_inside_a_literal_is_not_an_end_quote() -> None:
    sql = "select 'it''s fine; really' as x;"
    assert split_statements(sql) == ["select 'it''s fine; really' as x"]


def test_a_double_dash_inside_a_literal_is_not_a_comment() -> None:
    sql = "select 'a -- b' as x;"
    assert split_statements(sql) == ["select 'a -- b' as x"]


def test_line_comments_are_stripped() -> None:
    sql = "-- a comment with a semicolon; and an apostrophe's worth\nselect 1;"
    assert split_statements(sql) == ["select 1"]


def test_trailing_comment_on_a_code_line_is_stripped() -> None:
    assert split_statements("select 1 -- why\n;") == ["select 1"]


def test_the_real_ddl_parses_into_plausible_statements() -> None:
    """The file that actually ships is the case that matters."""
    statements = split_statements(DEFAULT_DDL.read_text(encoding="utf-8"))

    assert len(statements) >= 10
    assert all(statement for statement in statements), "no empty statements"
    verbs = {statement.split()[0].upper() for statement in statements}
    assert verbs <= {"CREATE", "ALTER"}, f"unexpected DDL verb: {verbs}"

    created = [s for s in statements if s.upper().startswith("CREATE TABLE")]
    assert len(created) == 6, "five bronze tables plus the ops quarantine table"


def test_default_ddl_ships_with_the_package() -> None:
    assert isinstance(DEFAULT_DDL, Path)
    assert DEFAULT_DDL.is_file()
