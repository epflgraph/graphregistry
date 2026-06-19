# tests/unit_tests/application/test_ops_edge.py
"""Unit tests for EdgeOperations using a fake repository adapter."""
from __future__ import annotations

from graphregistry.application.operations.ops_edge import EdgeOperations
from graphregistry.domain.models.entities.mdl_base import EdgeKeyList
from graphregistry.domain.models.entities.mdl_edge import EdgeList
from tests.conftest import FakeEdgeRepository, make_edge


class TestEdgeOperationsCrud:
    def test_save_and_get(self, edge_ops: EdgeOperations) -> None:
        edge = make_edge(from_object_id="CS-433", to_object_id="p-1")
        saved = edge_ops.save(edge)
        assert saved.key == edge.key

        loaded = edge_ops.get(edge.key)
        assert loaded is not None
        assert loaded.key.to_tuple() == edge.key.to_tuple()

    def test_exists_many(self, edge_ops: EdgeOperations) -> None:
        edge1 = make_edge(from_object_id="CS-433", to_object_id="p-1")
        edge2 = make_edge(from_object_id="MATH-203", to_object_id="p-2")
        edge_ops.save_many(EdgeList(item_list=[edge1, edge2]))

        assert edge_ops.exists_many(EdgeKeyList(item_list=[edge1.key, edge2.key])) == [True, True]

    def test_list_with_pattern(self, edge_ops: EdgeOperations) -> None:
        edge_ops.save(make_edge(from_object_id="CS-433", to_object_id="p-1"))
        edge_ops.save(make_edge(from_object_id="CS-250", to_object_id="p-2"))
        edge_ops.save(make_edge(from_object_id="MATH-203", to_object_id="p-3"))

        rows = edge_ops.list(object_type=("Course", "Person"), id_pattern="CS*")
        assert len(rows) == 2

    def test_delete(self, edge_ops: EdgeOperations) -> None:
        edge = make_edge(from_object_id="CS-433", to_object_id="p-1")
        edge_ops.save(edge)
        assert edge_ops.exists(edge.key) is True

        deleted = edge_ops.delete(edge.key)
        assert deleted is True
        assert edge_ops.exists(edge.key) is False


class TestEdgeOperationsUpsertSemantics:
    def test_upsert_creates_when_missing(self, edge_ops: EdgeOperations) -> None:
        edge = make_edge(from_object_id="CS-433", to_object_id="p-1")
        result = edge_ops.upsert(edge)
        assert result.success is True
        assert result.created is True

    def test_upsert_updates_when_existing(self, edge_ops: EdgeOperations) -> None:
        edge = make_edge(from_object_id="CS-433", to_object_id="p-1")
        edge_ops.save(edge)
        result = edge_ops.upsert(edge)
        assert result.success is True
        assert result.created is False

    def test_insert_and_update_are_aliases(self, edge_ops: EdgeOperations) -> None:
        edge = make_edge(from_object_id="CS-433", to_object_id="p-1")
        assert edge_ops.insert(edge) is True
        assert edge_ops.update(edge) is True
