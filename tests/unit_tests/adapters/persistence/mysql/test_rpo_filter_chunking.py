# graphregistry/tests/unit_tests/adapters/persistence/mysql/test_rpo_filter_chunking.py
"""Unit tests for the chunked existence filtering in the node and edge repositories."""
from __future__ import annotations
from typing import Any
from graphregistry.adapters.persistence.mysql.repositories.rpo_edgerepo import MySQLEdgeRepository
from graphregistry.adapters.persistence.mysql.repositories.rpo_noderepo import MySQLNodeRepository
from graphregistry.domain.models.entities.mdl_edge import EdgeKey
from graphregistry.domain.models.entities.mdl_node import NodeKey

# Internal Function: Build n node keys of one object type.
def _node_keys(n: int) -> list[NodeKey]:
    return [NodeKey(object_type="Publication", object_id=f"id{i}") for i in range(n)]

# Internal Function: Build n edge keys with unique from ids.
def _edge_keys(n: int) -> list[EdgeKey]:
    return [
        EdgeKey(from_object_type="Course", from_object_id=f"c{i}", to_object_type="Person", to_object_id="p-1", context="taught_by")
        for i in range(n)
    ]

#==================#
# Class Definition #
#==================#
class StubSchemaResolver:
    """Resolves every node and edge to one fixed engine and schema."""

    # Public Method: Resolve a node key to the fixed engine and schema.
    def for_node(self, key: NodeKey) -> tuple[str, str]:
        return ("coresrv", "stub_schema")

    # Public Method: Resolve an edge key to the fixed engine and schema.
    def for_edge(self, key: EdgeKey) -> tuple[str, str]:
        return ("coresrv", "stub_schema")

#==================#
# Class Definition #
#==================#
class FakeFilterSession:
    """Executes canned row batches, one per statement, recording every call."""

    # Public Method: Store the canned rows served back per executed statement.
    def __init__(self, rows_per_statement: list[list[tuple]]) -> None:
        self.rows_per_statement = rows_per_statement
        self.calls: list[dict[str, Any]] = []

    # Public Method: Record the executed statement and return its canned rows.
    def execute(self, query: str, params: dict[str, Any] | None = None) -> list[tuple]:
        index = len(self.calls)
        self.calls.append({"query": query, "params": params or {}})
        return self.rows_per_statement[index] if index < len(self.rows_per_statement) else []

    # Public Method: Close the fake session without action.
    def close(self) -> None:
        pass

#==================#
# Class Definition #
#==================#
class TestNodeFilterExistingKeysChunking:
    """Tests for the chunked existence filter of the node repository."""

    # Public Method: A large key batch is verified through three chunked
    # statements and the surviving keys keep their original order.
    def test_large_batch_chunked_and_ordered(self) -> None:
        keys = _node_keys(1200)
        existing = [(key.object_type, key.object_id) for key in keys[:1000]]

        # Serve the first two chunks as existing and the third as empty, so
        # exactly the first 1,000 keys must survive the filter.
        fake = FakeFilterSession([existing[:500], existing[500:], []])
        repo = MySQLNodeRepository(db=object(), schema_resolver=StubSchemaResolver())
        repo._session = lambda engine_name: fake

        # Run the filter over the full key batch.
        result = repo._filter_existing_keys(fake, "stub_schema", keys)

        # Chunks of 500/500/200 bind two parameters per key, restarting the
        # parameter indices at zero within every chunk.
        assert len(fake.calls) == 3
        expected_param_counts = [500 * 2, 500 * 2, 200 * 2]
        for call, expected_count in zip(fake.calls, expected_param_counts):
            assert len(call["params"]) == expected_count
            assert "ex_object_type_0" in call["params"]
        assert result == keys[:1000]

    # Public Method: A batch at or below the chunk cap issues a single statement.
    def test_small_batch_single_statement(self) -> None:
        keys = _node_keys(300)

        # Every key exists, so the single chunked statement returns them all.
        fake = FakeFilterSession([[(key.object_type, key.object_id) for key in keys]])
        repo = MySQLNodeRepository(db=object(), schema_resolver=StubSchemaResolver())
        repo._session = lambda engine_name: fake

        # Run the filter over the small batch.
        result = repo._filter_existing_keys(fake, "stub_schema", keys)

        # One statement covers the batch and every key survives.
        assert len(fake.calls) == 1
        assert result == keys

#==================#
# Class Definition #
#==================#
class TestEdgeFilterExistingKeysChunking:
    """Tests for the chunked existence filter of the edge repository."""

    # Public Method: A large edge key batch is verified through chunked
    # statements with five bind parameters per key.
    def test_large_batch_chunked(self) -> None:
        keys = _edge_keys(600)

        # Serve the first chunk (500 keys) as existing and the tail as absent.
        fake = FakeFilterSession([
            [key.to_tuple() for key in keys[:500]],
        ])
        repo = MySQLEdgeRepository(db=object(), schema_resolver=StubSchemaResolver())
        repo._session = lambda engine_name: fake

        # Run the filter over the full edge key batch.
        result = repo._filter_existing_keys(fake, "stub_schema", keys)

        # Two chunked statements verify the batch; only the first chunk exists.
        assert len(fake.calls) == 2
        assert len(fake.calls[0]["params"]) == 500 * 5
        assert len(fake.calls[1]["params"]) == 100 * 5
        assert result == keys[:500]
