# graphregistry/tests/unit_tests/common/test_logger.py
"""Unit tests for the GraphLogger single-item and aggregated batch lines."""
from __future__ import annotations
import pytest
from graphregistry.common.logger import GraphLogger
from graphregistry.domain.models.entities.mdl_edge import EdgeKey
from graphregistry.domain.models.entities.mdl_node import NodeKey

# Internal Function: Build n node keys of one object type.
def _node_keys(object_type: str, n: int) -> list[NodeKey]:
    return [NodeKey(object_type=object_type, object_id=f"id{i}") for i in range(n)]

# Internal Function: Build n edge keys sharing one from/to/context triple.
def _edge_keys(n: int) -> list[EdgeKey]:
    return [
        EdgeKey(from_object_type="Course", from_object_id=f"c{i}", to_object_type="Person", to_object_id="p-1", context="taught_by")
        for i in range(n)
    ]

#==================#
# Class Definition #
#==================#
class TestGraphLoggerBatchLines:
    """Tests for the one-line-per-batch aggregation of GraphLogger."""

    # Public Method: A single-type node batch logs one compact line with the total count.
    def test_saved_many_single_type(self, capsys: pytest.CaptureFixture) -> None:
        GraphLogger().saved_many(_node_keys("Publication", 1250))
        output = capsys.readouterr().out
        assert output.count("Saved:") == 1
        assert "1250 Nodes" in output
        assert "(Publication)" in output
        assert "×" not in output

    # Public Method: A mixed node batch logs one line with a count-sorted breakdown.
    def test_saved_many_mixed_types(self, capsys: pytest.CaptureFixture) -> None:
        keys = _node_keys("Course", 2) + _node_keys("Person", 1)
        GraphLogger().saved_many(keys)
        output = capsys.readouterr().out
        assert output.count("Saved:") == 1
        assert "3 Nodes" in output
        assert "(Course ×2, Person ×1)" in output

    # Public Method: An edge batch logs one line with the from/to/context triple.
    def test_saved_many_edge_keys(self, capsys: pytest.CaptureFixture) -> None:
        GraphLogger().saved_many(_edge_keys(2))
        output = capsys.readouterr().out
        assert output.count("Saved:") == 1
        assert "2 Edges" in output
        assert "(Course, Person, taught_by)" in output

    # Public Method: A deletion batch logs one aggregated line like the save batch.
    def test_deleted_many_mixed_types(self, capsys: pytest.CaptureFixture) -> None:
        keys = _node_keys("Publication", 3750)
        GraphLogger().deleted_many(keys)
        output = capsys.readouterr().out
        assert output.count("Deleted:") == 1
        assert "3750 Nodes" in output
        assert "(Publication)" in output

    # Public Method: An empty batch logs nothing.
    def test_empty_batch_logs_nothing(self, capsys: pytest.CaptureFixture) -> None:
        GraphLogger().saved_many([])
        GraphLogger().deleted_many([])
        assert capsys.readouterr().out == ""

    # Public Method: Single-item saves keep their detailed per-node line format.
    def test_single_save_line_unchanged(self, capsys: pytest.CaptureFixture) -> None:
        GraphLogger().saved(NodeKey(object_type="Publication", object_id="id0"))
        output = capsys.readouterr().out
        assert output.count("Saved:") == 1
        assert "Node" in output
        assert "(Publication, id0)" in output
