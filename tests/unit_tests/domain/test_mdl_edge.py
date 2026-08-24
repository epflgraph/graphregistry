# tests/unit_tests/domain/test_mdl_edge.py
"""Unit tests for the Edge aggregate and its value objects."""
from __future__ import annotations

from graphregistry.domain.models.entities.mdl_base import EdgeFieldKey, EdgeKey
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeField, EdgeFieldList, EdgeList


class TestEdgeField:
    def test_edge_field_from_json(self) -> None:
        key = EdgeKey(
            from_object_type="Course", from_object_id="CS-433",
            to_object_type="Person", to_object_id="p-1",
            context="taught_by",
        )
        field = EdgeField.from_json(
            {"field_language": "en", "field_name": "semester", "field_value": "fall"},
            edge_key=key,
        )
        assert field.key.key == key
        assert field.field_value == "fall"


class TestEdge:
    def test_edge_json_roundtrip(self) -> None:
        key = EdgeKey(
            from_object_type="Course", from_object_id="CS-433",
            to_object_type="Person", to_object_id="p-1",
            context="taught_by",
        )
        field = EdgeField(
            key=EdgeFieldKey(key=key, field_language="en", field_name="semester"),
            field_value="fall",
        )
        edge = Edge(key=key, field_list=EdgeFieldList(item_list=[field]))
        rebuilt = Edge.from_json(edge.to_json())
        assert rebuilt.key.to_tuple() == key.to_tuple()
        assert len(rebuilt.field_list.item_list) == 1


class TestEdgeList:
    def test_edge_list_from_list(self) -> None:
        key = EdgeKey(
            from_object_type="Course", from_object_id="CS-433",
            to_object_type="Person", to_object_id="p-1",
            context="taught_by",
        )
        edge_list = EdgeList(item_list=[Edge(key=key)])
        rebuilt = EdgeList.from_list(edge_list.to_list())
        assert len(rebuilt.item_list) == 1
