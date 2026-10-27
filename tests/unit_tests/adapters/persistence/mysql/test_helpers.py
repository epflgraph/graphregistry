# graphregistry/tests/unit_tests/adapters/persistence/mysql/test_helpers.py
"""Unit tests for the fixed-size statement chunking of the MySQL batch helpers."""
from __future__ import annotations
from typing import Any
from graphregistry.adapters.persistence.mysql.repositories.helpers import soft_delete_by_key_tuples, upsert_rows

# Internal Function: Build n plain rows with three columns for upsert tests.
def _make_rows(n: int) -> list[dict[str, Any]]:
    return [
        {"object_type": "Publication", "object_id": f"id{i}", "object_title": f"title{i}"}
        for i in range(n)
    ]

# Internal Function: Build n two-column key tuples for soft-delete tests.
def _make_keys(n: int) -> list[tuple[str, str]]:
    return [("Publication", f"id{i}") for i in range(n)]

# Internal Function: Count the value tuples rendered on the VALUES line of an INSERT.
def _value_tuple_count(sql: str) -> int:
    values_line = next(line for line in sql.splitlines() if "VALUES" in line)
    return values_line.count("(")

# Internal Function: Normalize the INSERT header (table and column list) of a statement.
def _normalized_header(sql: str) -> str:
    return " ".join(sql.split("VALUES")[0].split())

# Internal Function: Normalize the ON DUPLICATE KEY UPDATE clause of a statement.
def _normalized_update_clause(sql: str) -> str:
    return " ".join(sql.split("ON DUPLICATE KEY UPDATE", 1)[1].split())

#==================#
# Class Definition #
#==================#
class FakeSession:
    """Captures every executed statement so tests can assert chunking behavior."""

    # Public Method: Initialize the captured statement list.
    def __init__(self) -> None:
        self.statements: list[tuple[str, dict[str, Any]]] = []

    # Public Method: Record the SQL text and parameters of each executed statement.
    def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[tuple[Any, ...]]:
        self.statements.append((sql, params or {}))
        return []

#==================#
# Class Definition #
#==================#
class TestUpsertRowsChunking:
    """Tests for the fixed-size statement chunking of the batch upsert helper."""

    # Public Method: A batch at or below the cap renders exactly one statement.
    def test_small_batch_single_statement(self) -> None:
        session = FakeSession()
        upsert_rows(session, "`reg`.`Nodes_N_Object`", ["object_type", "object_id"], ["object_title"], _make_rows(300))
        assert len(session.statements) == 1
        sql, params = session.statements[0]
        assert _value_tuple_count(sql) == 300
        assert len(params) == 300 * 3
        assert "object_title_299" in params
        assert "object_title_300" not in params

    # Public Method: A large batch is split into fixed-size chunks with a smaller remainder.
    def test_large_batch_chunked(self) -> None:
        session = FakeSession()
        upsert_rows(session, "`reg`.`Nodes_N_Object`", ["object_type", "object_id"], ["object_title"], _make_rows(1200))
        assert len(session.statements) == 3
        counts = [_value_tuple_count(sql) for sql, _ in session.statements]
        assert counts == [500, 500, 200]
        for sql, params in session.statements:
            assert len(params) == _value_tuple_count(sql) * 3

    # Public Method: A batch exactly matching the cap splits into full chunks without a tail.
    def test_exact_multiple_of_chunk_size(self) -> None:
        session = FakeSession()
        upsert_rows(session, "`reg`.`Nodes_N_Object`", ["object_type", "object_id"], ["object_title"], _make_rows(1000))
        assert [_value_tuple_count(sql) for sql, _ in session.statements] == [500, 500]

    # Public Method: Every chunk restarts its bind-parameter indices at zero.
    def test_bind_indices_restart_per_chunk(self) -> None:
        session = FakeSession()
        upsert_rows(session, "`reg`.`Nodes_N_Object`", ["object_type", "object_id"], ["object_title"], _make_rows(1200))
        for sql, params in session.statements:
            assert "object_type_0" in params
            assert "object_title_0" in params

    # Public Method: All chunks of one batch share an identical statement shape so the
    # compiled-statement cache is hit on every chunk after the first.
    def test_chunks_share_statement_shape(self) -> None:
        session = FakeSession()
        upsert_rows(session, "`reg`.`Nodes_N_Object`", ["object_type", "object_id"], ["object_title"], _make_rows(1200))
        headers = {_normalized_header(sql) for sql, _ in session.statements}
        updates = {_normalized_update_clause(sql) for sql, _ in session.statements}
        assert len(headers) == 1
        assert len(updates) == 1

    # Public Method: An empty row list executes no statement.
    def test_empty_rows_execute_nothing(self) -> None:
        session = FakeSession()
        upsert_rows(session, "`reg`.`Nodes_N_Object`", ["object_type", "object_id"], ["object_title"], [])
        assert session.statements == []

#==================#
# Class Definition #
#==================#
class TestSoftDeleteByKeyTuplesChunking:
    """Tests for the fixed-size statement chunking of the soft-delete helper."""

    # Public Method: A large key batch is split into fixed-size IN-list statements.
    def test_large_key_batch_chunked(self) -> None:
        session = FakeSession()
        soft_delete_by_key_tuples(session, "reg", "Data_N_Object_T_CustomFields", ["object_type", "object_id"], _make_keys(1200))
        assert len(session.statements) == 3
        for sql, params in session.statements:
            assert "record_deleted = 1" in sql
            assert len(params) == 500 * 2 or len(params) == 200 * 2

    # Public Method: A key batch at or below the cap renders exactly one statement.
    def test_small_key_batch_single_statement(self) -> None:
        session = FakeSession()
        soft_delete_by_key_tuples(session, "reg", "Data_N_Object_T_CustomFields", ["object_type", "object_id"], _make_keys(300))
        assert len(session.statements) == 1
        sql, params = session.statements[0]
        assert len(params) == 300 * 2
        assert "sd_object_id_299" in params

    # Public Method: An empty key list executes no statement.
    def test_empty_keys_execute_nothing(self) -> None:
        session = FakeSession()
        soft_delete_by_key_tuples(session, "reg", "Data_N_Object_T_CustomFields", ["object_type", "object_id"], [])
        assert session.statements == []
