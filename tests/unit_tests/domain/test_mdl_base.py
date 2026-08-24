# tests/unit_tests/domain/test_mdl_base.py
"""Unit tests for shared base domain entities and value objects."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from graphregistry.domain.models.entities.mdl_base import (
    EdgeFieldKey,
    EdgeFieldKeyList,
    EdgeKey,
    EdgeKeyList,
    NodeFieldKey,
    NodeFieldKeyList,
    NodeKey,
    NodeKeyList,
)


class TestNodeKey:
    def test_node_key_from_dict(self) -> None:
        key = NodeKey(institution_id="EPFL", object_type="Course", object_id="CS-433")
        assert key.institution_id == "EPFL"
        assert key.object_type == "Course"
        assert key.object_id == "CS-433"
        assert key.to_tuple() == ("EPFL", "Course", "CS-433")

    def test_node_key_from_tuple(self) -> None:
        key = NodeKey.from_tuple(("EPFL", "Person", "p-1"))
        assert key.object_type == "Person"
        assert key.object_id == "p-1"

    def test_node_key_from_tuple_wrong_length(self) -> None:
        with pytest.raises(ValidationError):
            NodeKey.model_validate(("EPFL", "Course"))

    def test_node_key_is_frozen(self) -> None:
        key = NodeKey(institution_id="EPFL", object_type="Course", object_id="CS-433")
        with pytest.raises(ValidationError):
            key.object_id = "MATH-203"


class TestNodeKeyList:
    def test_from_tuple_list(self) -> None:
        key_list = NodeKeyList.from_tuple_list([("EPFL", "Course", "CS-433"), ("EPFL", "Course", "MATH-203")])
        assert len(key_list.item_list) == 2
        assert key_list.to_tuple_list() == [("EPFL", "Course", "CS-433"), ("EPFL", "Course", "MATH-203")]


class TestNodeFieldKey:
    def test_from_tuple(self) -> None:
        key = NodeFieldKey.from_tuple(("EPFL", "Course", "CS-433", "en", "level"))
        assert key.key.to_tuple() == ("EPFL", "Course", "CS-433")
        assert key.field_language == "en"
        assert key.field_name == "level"
        assert key.to_tuple() == ("EPFL", "Course", "CS-433", "en", "level")

    def test_from_tuple_wrong_length(self) -> None:
        with pytest.raises(ValueError, match="must have 5 elements"):
            NodeFieldKey.from_tuple(("EPFL", "Course", "CS-433", "en"))


class TestEdgeKey:
    def test_edge_key_from_dict(self) -> None:
        key = EdgeKey(
            from_institution_id="EPFL",
            from_object_type="Course",
            from_object_id="CS-433",
            to_institution_id="EPFL",
            to_object_type="Person",
            to_object_id="p-1",
            context="taught_by",
        )
        assert key.to_tuple() == ("EPFL", "Course", "CS-433", "EPFL", "Person", "p-1", "taught_by")

    def test_edge_key_from_tuple(self) -> None:
        key = EdgeKey.from_tuple(("EPFL", "Course", "CS-433", "EPFL", "Person", "p-1", "taught_by"))
        assert key.context == "taught_by"

    def test_edge_key_from_tuple_wrong_length(self) -> None:
        with pytest.raises(ValidationError):
            EdgeKey.model_validate(("EPFL", "Course", "CS-433"))


class TestEdgeKeyList:
    def test_from_tuple_list(self) -> None:
        key_list = EdgeKeyList.from_tuple_list([
            ("EPFL", "Course", "CS-433", "EPFL", "Person", "p-1", "taught_by"),
            ("EPFL", "Course", "MATH-203", "EPFL", "Person", "p-2", "taught_by"),
        ])
        assert len(key_list.item_list) == 2


class TestEdgeFieldKey:
    def test_from_tuple(self) -> None:
        key = EdgeFieldKey.from_tuple(("EPFL", "Course", "CS-433", "EPFL", "Person", "p-1", "taught_by", "en", "semester"))
        assert key.key.context == "taught_by"
        assert key.field_language == "en"
        assert key.field_name == "semester"

    def test_from_tuple_wrong_length(self) -> None:
        with pytest.raises(ValueError, match="must have 9 elements"):
            EdgeFieldKey.from_tuple(("EPFL", "Course", "CS-433", "EPFL", "Person", "p-1", "taught_by"))
