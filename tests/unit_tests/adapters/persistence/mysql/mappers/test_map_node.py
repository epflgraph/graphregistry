# tests/unit_tests/adapters/persistence/mysql/mappers/test_map_node.py
"""Unit tests for the MySQL node mapper using synthetic row data."""
from __future__ import annotations

from graphregistry.adapters.persistence.mysql.mappers.map_node import MySQLNodeFieldMapper, MySQLNodeMapper
from graphregistry.domain.models.entities.mdl_base import NodeFieldKey, NodeKey
from graphregistry.domain.models.entities.mdl_conceptmap import Concept, ScoredConcept, ScoredConceptList
from graphregistry.domain.models.entities.mdl_node import Node, NodeConceptList, NodeField, NodeFieldList


class TestMySQLNodeFieldMapper:
    def test_from_row(self) -> None:
        key = NodeKey(object_type="Course", object_id="CS-433")
        field = MySQLNodeFieldMapper.from_row(("en", "level", "master"), node_key=key)
        assert field.key == NodeFieldKey(key=key, field_language="en", field_name="level")
        assert field.field_value == "master"

    def test_from_rows(self) -> None:
        key = NodeKey(object_type="Course", object_id="CS-433")
        field_list = MySQLNodeFieldMapper.from_rows([("en", "level", "master"), ("fr", "niveau", "master")], node_key=key)
        assert len(field_list.item_list) == 2

    def test_to_upsert_row(self) -> None:
        key = NodeKey(object_type="Course", object_id="CS-433")
        field = NodeField(key=NodeFieldKey(key=key, field_language="en", field_name="level"), field_value="master")
        row = MySQLNodeFieldMapper.to_upsert_row(field)
        assert row == {
            "object_type": "Course",
            "object_id": "CS-433",
            "field_language": "en",
            "field_name": "level",
            "field_value": "master",
            "record_deleted": 0,
        }


class TestMySQLNodeMapper:
    def test_from_parts_basic(self) -> None:
        key = NodeKey(object_type="Course", object_id="CS-433")
        node = MySQLNodeMapper.from_parts(
            key=key,
            basic_row=("Machine Learning", "user input", "Learn ML."),
        )
        assert node.title == "Machine Learning"
        assert node.text_source == "user input"
        assert node.raw_text == "Learn ML."

    def test_from_parts_with_concepts(self) -> None:
        key = NodeKey(object_type="Course", object_id="CS-433")
        node = MySQLNodeMapper.from_parts(
            key=key,
            basic_row=("Machine Learning", "user input", "Learn ML."),
            detected_concept_rows=[("c1", 0.9)],
        )
        assert len(node.concepts.detected.item_list) == 1
        assert node.concepts.detected.item_list[0].concept.id == "c1"
        assert node.concepts.detected.item_list[0].score == 0.9

    def test_to_basic_row(self) -> None:
        key = NodeKey(object_type="Course", object_id="CS-433")
        node = Node(key=key, title="ML", text_source="user", raw_text="text")
        assert MySQLNodeMapper.to_basic_row(node) == {
            "object_title": "ML",
            "text_source": "user",
            "raw_text": "text",
            "record_deleted": 0,
        }

    def test_to_scored_concepts_rows(self) -> None:
        key = NodeKey(object_type="Course", object_id="CS-433")
        node = Node(
            key=key,
            concepts=NodeConceptList(
                detected=ScoredConceptList(item_list=[
                    ScoredConcept(concept=Concept(id="c1", name="ML"), score=0.9),
                ])
            )
        )
        rows = MySQLNodeMapper.to_scored_concepts_rows(node, "detected")
        assert rows == [{
            "object_type": "Course",
            "object_id": "CS-433",
            "concept_id": "c1",
            "text_source": "detected",
            "score": 0.9,
            "record_deleted": 0,
        }]

    def test_simplified_dict_roundtrip(self) -> None:
        key = NodeKey(object_type="Course", object_id="CS-433")
        original = Node(key=key, title="ML")
        original.page_profile.short_code = "ml"
        original.page_profile.name.set("en", "Machine Learning")

        data = MySQLNodeMapper.to_simplified_dict(original)
        rebuilt = MySQLNodeMapper.from_simplified_dict(data)
        assert rebuilt.key == key
        assert rebuilt.title == "ML"
        assert rebuilt.page_profile.short_code == "ml"
        assert rebuilt.page_profile.name.get_value("en") == "Machine Learning"
