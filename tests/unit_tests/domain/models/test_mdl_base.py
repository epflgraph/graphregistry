from __future__ import annotations

import pytest

from graphregistry.domain.models.entities.mdl_base import EdgeFieldKey, EdgeKey, NodeFieldKey, NodeKey


def test_node_key_from_tuple_to_tuple_and_dict() -> None:
    key = NodeKey.from_tuple(("EPFL", "Course", "CS-101"))

    assert key.to_tuple() == ("EPFL", "Course", "CS-101")
    # assert key.to_dict() == {
    #     "institution_id": "EPFL",
    #     "object_type": "Course",
    #     "object_id": "CS-101",
    # }


def test_node_key_from_tuple_invalid_length_raises() -> None:
    with pytest.raises(ValueError, match="NodeKey tuple must have 3 elements"):
        NodeKey.model_validate(("EPFL", "Course"))


def test_node_field_key_to_tuple_and_dict() -> None:
    key = NodeFieldKey(
        key=NodeKey(institution_id="EPFL", object_type="Course", object_id="CS-101"),
        field_language="en",
        field_name="summary",
    )

    assert key.to_tuple() == ("EPFL", "Course", "CS-101", "en", "summary")
    # assert key.to_dict() == {
    #     "institution_id": "EPFL",
    #     "object_type": "Course",
    #     "object_id": "CS-101",
    #     "field_language": "en",
    #     "field_name": "summary",
    # }


def test_edge_key_from_tuple_to_tuple_and_dict() -> None:
    values = ("EPFL", "Course", "CS-101", "EPFL", "Person", "123", "teacher")
    key = EdgeKey.from_tuple(values)

    assert key.to_tuple() == values
    # assert key.to_dict() == {
    #     "from_institution_id": "EPFL",
    #     "from_object_type": "Course",
    #     "from_object_id": "CS-101",
    #     "to_institution_id": "EPFL",
    #     "to_object_type": "Person",
    #     "to_object_id": "123",
    #     "context": "teacher",
    # }


def test_edge_key_from_tuple_invalid_length_raises() -> None:
    with pytest.raises(ValueError, match="EdgeKey tuple must have 7 elements"):
        EdgeKey.model_validate(("EPFL", "Course", "CS-101"))


def test_edge_field_key_to_tuple_and_dict() -> None:
    key = EdgeFieldKey(
        key=EdgeKey(
            from_institution_id="EPFL",
            from_object_type="Course",
            from_object_id="CS-101",
            to_institution_id="EPFL",
            to_object_type="Person",
            to_object_id="123",
            context="teacher",
        ),
        field_language="en",
        field_name="role",
    )

    assert key.to_tuple() == (
        "EPFL",
        "Course",
        "CS-101",
        "EPFL",
        "Person",
        "123",
        "teacher",
        "en",
        "role",
    )
    # assert key.to_dict() == {
    #     "from_institution_id": "EPFL",
    #     "from_object_type": "Course",
    #     "from_object_id": "CS-101",
    #     "to_institution_id": "EPFL",
    #     "to_object_type": "Person",
    #     "to_object_id": "123",
    #     "context": "teacher",
    #     "field_language": "en",
    #     "field_name": "role",
    # }
