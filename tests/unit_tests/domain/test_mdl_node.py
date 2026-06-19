# tests/unit_tests/domain/test_mdl_node.py
"""Unit tests for the Node aggregate and its value objects."""
from __future__ import annotations

from graphregistry.domain.models.entities.mdl_base import NodeFieldKey, NodeKey
from graphregistry.domain.models.entities.mdl_node import Node, NodeConceptList, NodeField, NodeFieldList, NodeList
from graphregistry.domain.models.entities.mdl_pageprofile import PageProfile


class TestNodeField:
    def test_node_field_from_json(self) -> None:
        key = NodeKey(institution_id="EPFL", object_type="Course", object_id="CS-433")
        field = NodeField.from_json(
            {"field_language": "en", "field_name": "level", "field_value": "master"},
            node_key=key,
        )
        assert field.key.key == key
        assert field.key.field_name == "level"
        assert field.field_value == "master"


class TestNode:
    def test_node_default_page_profile(self) -> None:
        key = NodeKey(institution_id="EPFL", object_type="Course", object_id="CS-433")
        node = Node(key=key)
        assert node.page_profile is not None
        assert node.page_profile.key == key

    def test_node_field_keys_are_fixed_on_validation(self) -> None:
        key = NodeKey(institution_id="EPFL", object_type="Course", object_id="CS-433")
        wrong_key = NodeKey(institution_id="EPFL", object_type="Course", object_id="OLD-123")
        field = NodeField(key=NodeFieldKey(key=wrong_key, field_language="en", field_name="level"), field_value="master")
        node = Node(key=key, field_list=NodeFieldList(item_list=[field]))
        assert node.field_list.item_list[0].key.key == key

    def test_node_json_roundtrip(self) -> None:
        key = NodeKey(institution_id="EPFL", object_type="Course", object_id="CS-433")
        node = Node(
            key=key,
            title="Machine Learning",
            page_profile=PageProfile(key=key, short_code="ML"),
        )
        node.page_profile.name.set("en", "Machine Learning")
        rebuilt = Node.from_json(node.to_json())
        assert rebuilt.title == "Machine Learning"
        assert rebuilt.page_profile.short_code == "ML"
        assert rebuilt.page_profile.name.get_value("en") == "Machine Learning"

    def test_node_concepts_default_empty(self) -> None:
        key = NodeKey(institution_id="EPFL", object_type="Course", object_id="CS-433")
        node = Node(key=key)
        assert isinstance(node.concepts, NodeConceptList)
        assert node.concepts.detected.item_list == []


class TestNodeList:
    def test_node_list_from_list(self) -> None:
        key1 = NodeKey(institution_id="EPFL", object_type="Course", object_id="CS-433")
        key2 = NodeKey(institution_id="EPFL", object_type="Course", object_id="MATH-203")
        node_list = NodeList(item_list=[Node(key=key1), Node(key=key2)])
        rebuilt = NodeList.from_list(node_list.to_list())
        assert len(rebuilt.item_list) == 2
        assert rebuilt.item_list[0].key.object_id == "CS-433"
