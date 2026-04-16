# tests/unit_tests/adapters/persistence/mysql/mappers/test_amp_node.py
from __future__ import annotations

from graphregistry.adapters.persistence.mysql.mappers.amp_node import (
    MySQLNodeFieldMapper,
    MySQLNodeMapper,
)
from graphregistry.adapters.persistence.mysql.mappers.amp_pageprofile import MySQLPageProfileMapper
from graphregistry.domain.models.mdl_base import NodeFieldKey, NodeKey
from graphregistry.domain.models.mdl_node import Node, NodeField, NodeFieldList
from graphregistry.domain.models.mdl_pageprofile import PageProfile


def make_node_key() -> NodeKey:
    return NodeKey(
        institution_id="EPFL",
        object_type="Course",
        object_id="TEST-101",
    )


def make_node_field() -> NodeField:
    key = make_node_key()
    return NodeField(
        key=NodeFieldKey(
            key=key,
            field_language="en",
            field_name="summary",
        ),
        field_value="An introduction to autonomous systems.",
    )


def make_page_profile_row() -> dict[str, object]:
    return {
        "numeric_id_en": "101",
        "numeric_id_fr": "101",
        "short_code": "TEST-101",
        "subtype_en": "Course",
        "subtype_fr": "Cours",
        "name_en_is_auto_generated": 0,
        "name_en_is_auto_corrected": 0,
        "name_en_is_auto_translated": 0,
        "name_en_translated_from": None,
        "name_en_value": "Introduction to Autonomous Systems Design",
        "name_fr_is_auto_generated": 0,
        "name_fr_is_auto_corrected": 0,
        "name_fr_is_auto_translated": 1,
        "name_fr_translated_from": "en",
        "name_fr_value": "Introduction à la conception de systèmes autonomes",
        "description_short_en_is_auto_generated": 1,
        "description_short_en_is_auto_corrected": 1,
        "description_short_en_is_auto_translated": 0,
        "description_short_en_translated_from": None,
        "description_short_en_value": "Learn the fundamentals of autonomous systems.",
        "description_medium_en_is_auto_generated": 1,
        "description_medium_en_is_auto_corrected": 1,
        "description_medium_en_is_auto_translated": 0,
        "description_medium_en_translated_from": None,
        "description_medium_en_value": "Structured introduction to autonomous systems.",
        "description_long_en_is_auto_generated": 1,
        "description_long_en_is_auto_corrected": 1,
        "description_long_en_is_auto_translated": 0,
        "description_long_en_translated_from": None,
        "description_long_en_value": "Detailed long description.",
        "external_key_en": "intro-autonomous-systems-TEST-101",
        "external_url_en": "https://edu.epfl.ch/coursebook/en/intro-autonomous-systems-TEST-101",
        "is_visible": 1,
    }


def make_simplified_node_dict() -> dict[str, object]:
    return {
        "institution_id": "EPFL",
        "object_type": "Course",
        "object_id": "TEST-101",
        "object_title": "Introduction to Autonomous Systems Design",
        "text_source": "course page description",
        "raw_text": "Autonomous systems course raw text.",
        "custom_fields": [
            {
                "field_language": "en",
                "field_name": "summary",
                "field_value": "An introduction to autonomous systems.",
            },
            {
                "field_language": "n/a",
                "field_name": "course_code",
                "field_value": "TEST-101",
            },
        ],
        "page_profile": {
            "short_code": "TEST-101",
            "name_en_value": "Introduction to Autonomous Systems Design",
            "name_fr_value": "Introduction à la conception de systèmes autonomes",
            "description_short_en_value": "Learn the fundamentals of autonomous systems.",
            "description_medium_en_value": "Structured introduction to autonomous systems.",
            "description_long_en_value": "Detailed long description.",
            "external_key_en": "intro-autonomous-systems-TEST-101",
            "external_url_en": "https://edu.epfl.ch/coursebook/en/intro-autonomous-systems-TEST-101",
            "is_visible": True,
        },
    }


def make_node() -> Node:
    data = make_simplified_node_dict()
    return MySQLNodeMapper.from_simplified_dict(data)


# ======================== #
# MySQLNodeFieldMapper     #
# ======================== #

def test_node_field_mapper_from_row() -> None:
    key = make_node_key()
    row = ("en", "summary", "An introduction to autonomous systems.")

    field = MySQLNodeFieldMapper.from_row(row, node_key=key)

    assert isinstance(field, NodeField)
    assert field.key.key == key
    assert field.key.field_language == "en"
    assert field.key.field_name == "summary"
    assert field.field_value == "An introduction to autonomous systems."


def test_node_field_mapper_from_row_normalizes_none() -> None:
    key = make_node_key()
    row = (None, None, None)

    field = MySQLNodeFieldMapper.from_row(row, node_key=key)

    assert field.key.key == key
    assert field.key.field_language == ""
    assert field.key.field_name == ""
    assert field.field_value is None


def test_node_field_mapper_from_dict() -> None:
    key = make_node_key()
    row = {
        "field_language": "fr",
        "field_name": "summary",
        "field_value": "Introduction aux systèmes autonomes.",
    }

    field = MySQLNodeFieldMapper.from_dict(row, node_key=key)

    assert field.key.key == key
    assert field.key.field_language == "fr"
    assert field.key.field_name == "summary"
    assert field.field_value == "Introduction aux systèmes autonomes."


def test_node_field_mapper_from_dict_normalizes_missing_values() -> None:
    key = make_node_key()
    row = {}

    field = MySQLNodeFieldMapper.from_dict(row, node_key=key)

    assert field.key.key == key
    assert field.key.field_language == ""
    assert field.key.field_name == ""
    assert field.field_value is None


def test_node_field_mapper_from_rows() -> None:
    key = make_node_key()
    rows = [
        ("en", "summary", "English summary"),
        ("fr", "summary", "Résumé français"),
    ]

    field_list = MySQLNodeFieldMapper.from_rows(rows, node_key=key)

    assert isinstance(field_list, NodeFieldList)
    assert len(field_list.field_list) == 2
    assert field_list.field_list[0].key.key == key
    assert field_list.field_list[0].key.field_language == "en"
    assert field_list.field_list[1].key.field_language == "fr"


def test_node_field_mapper_from_rows_with_none() -> None:
    key = make_node_key()

    field_list = MySQLNodeFieldMapper.from_rows(None, node_key=key)

    assert isinstance(field_list, NodeFieldList)
    assert field_list.field_list == []


def test_node_field_mapper_from_dicts() -> None:
    key = make_node_key()
    rows = [
        {"field_language": "en", "field_name": "summary", "field_value": "English summary"},
        {"field_language": "n/a", "field_name": "course_code", "field_value": "TEST-101"},
    ]

    field_list = MySQLNodeFieldMapper.from_dicts(rows, node_key=key)

    assert isinstance(field_list, NodeFieldList)
    assert len(field_list.field_list) == 2
    assert field_list.field_list[0].key.field_name == "summary"
    assert field_list.field_list[1].key.field_name == "course_code"


def test_node_field_mapper_from_dicts_with_none() -> None:
    key = make_node_key()

    field_list = MySQLNodeFieldMapper.from_dicts(None, node_key=key)

    assert isinstance(field_list, NodeFieldList)
    assert field_list.field_list == []


def test_node_field_mapper_to_upsert_row() -> None:
    field = make_node_field()

    row = MySQLNodeFieldMapper.to_upsert_row(field)

    assert row == {
        "institution_id": "EPFL",
        "object_type": "Course",
        "object_id": "TEST-101",
        "field_language": "en",
        "field_name": "summary",
        "field_value": "An introduction to autonomous systems.",
    }


def test_node_field_mapper_to_dict() -> None:
    field = make_node_field()

    row = MySQLNodeFieldMapper.to_dict(field)

    assert row == {
        "institution_id": "EPFL",
        "object_type": "Course",
        "object_id": "TEST-101",
        "field_language": "en",
        "field_name": "summary",
        "field_value": "An introduction to autonomous systems.",
    }


def test_node_field_mapper_to_simplified_row() -> None:
    field = make_node_field()

    row = MySQLNodeFieldMapper.to_simplified_row(field)

    assert row == {
        "field_language": "en",
        "field_name": "summary",
        "field_value": "An introduction to autonomous systems.",
    }


def test_node_field_mapper_to_upsert_rows() -> None:
    key = make_node_key()
    field_list = NodeFieldList(
        field_list=[
            NodeField(
                key=NodeFieldKey(key=key, field_language="en", field_name="summary"),
                field_value="English summary",
            ),
            NodeField(
                key=NodeFieldKey(key=key, field_language="n/a", field_name="course_code"),
                field_value="TEST-101",
            ),
        ]
    )

    rows = MySQLNodeFieldMapper.to_upsert_rows(field_list)

    assert rows == [
        {
            "institution_id": "EPFL",
            "object_type": "Course",
            "object_id": "TEST-101",
            "field_language": "en",
            "field_name": "summary",
            "field_value": "English summary",
        },
        {
            "institution_id": "EPFL",
            "object_type": "Course",
            "object_id": "TEST-101",
            "field_language": "n/a",
            "field_name": "course_code",
            "field_value": "TEST-101",
        },
    ]


def test_node_field_mapper_to_dicts() -> None:
    key = make_node_key()
    field_list = NodeFieldList(
        field_list=[
            NodeField(
                key=NodeFieldKey(key=key, field_language="en", field_name="summary"),
                field_value="English summary",
            )
        ]
    )

    rows = MySQLNodeFieldMapper.to_dicts(field_list)

    assert rows == [
        {
            "institution_id": "EPFL",
            "object_type": "Course",
            "object_id": "TEST-101",
            "field_language": "en",
            "field_name": "summary",
            "field_value": "English summary",
        }
    ]


def test_node_field_mapper_to_simplified_rows() -> None:
    key = make_node_key()
    field_list = NodeFieldList(
        field_list=[
            NodeField(
                key=NodeFieldKey(key=key, field_language="en", field_name="summary"),
                field_value="English summary",
            ),
            NodeField(
                key=NodeFieldKey(key=key, field_language="fr", field_name="summary"),
                field_value="Résumé français",
            ),
        ]
    )

    rows = MySQLNodeFieldMapper.to_simplified_rows(field_list)

    assert rows == [
        {
            "field_language": "en",
            "field_name": "summary",
            "field_value": "English summary",
        },
        {
            "field_language": "fr",
            "field_name": "summary",
            "field_value": "Résumé français",
        },
    ]


# ======================== #
# MySQLNodeMapper          #
# ======================== #

def test_node_mapper_from_parts_full() -> None:
    key = make_node_key()
    basic_row = (
        "Introduction to Autonomous Systems Design",
        "course page description",
        "Autonomous systems course raw text.",
    )
    custom_field_rows = [
        ("en", "summary", "An introduction to autonomous systems."),
        ("n/a", "course_code", "TEST-101"),
    ]
    page_profile_row = make_page_profile_row()

    node = MySQLNodeMapper.from_parts(
        key=key,
        basic_row=basic_row,
        custom_field_rows=custom_field_rows,
        page_profile_row=page_profile_row,
    )

    assert isinstance(node, Node)
    assert node.key == key
    assert node.title == "Introduction to Autonomous Systems Design"
    assert node.text_source == "course page description"
    assert node.raw_text == "Autonomous systems course raw text."
    assert len(node.field_list.field_list) == 2

    assert node.page_profile is not None
    assert node.page_profile.key == key
    assert node.page_profile.short_code == "TEST-101"
    assert node.page_profile.name.en.value == "Introduction to Autonomous Systems Design"
    assert node.page_profile.name.fr.value == "Introduction à la conception de systèmes autonomes"
    assert node.page_profile.description.short.en.value == "Learn the fundamentals of autonomous systems."
    assert node.page_profile.external_key.en == "intro-autonomous-systems-TEST-101"
    assert node.page_profile.external_url.en == "https://edu.epfl.ch/coursebook/en/intro-autonomous-systems-TEST-101"
    assert node.page_profile.is_visible is True


def test_node_mapper_from_parts_with_none_basic_row() -> None:
    key = make_node_key()

    node = MySQLNodeMapper.from_parts(
        key=key,
        basic_row=None,
        custom_field_rows=None,
        page_profile_row=None,
    )

    assert node.key == key
    assert node.title == ""
    assert node.text_source == ""
    assert node.raw_text == ""
    assert node.field_list.field_list == []
    assert node.page_profile is not None
    assert node.page_profile.key == key
    assert node.page_profile.short_code == ""
    assert node.page_profile.is_visible is True


def test_node_mapper_to_basic_row() -> None:
    node = make_node()

    row = MySQLNodeMapper.to_basic_row(node)

    assert row == {
        "object_title": "Introduction to Autonomous Systems Design",
        "text_source": "course page description",
        "raw_text": "Autonomous systems course raw text.",
    }


def test_node_mapper_to_custom_field_rows() -> None:
    node = make_node()

    rows = MySQLNodeMapper.to_custom_field_rows(node)

    assert rows == [
        {
            "institution_id": "EPFL",
            "object_type": "Course",
            "object_id": "TEST-101",
            "field_language": "en",
            "field_name": "summary",
            "field_value": "An introduction to autonomous systems.",
        },
        {
            "institution_id": "EPFL",
            "object_type": "Course",
            "object_id": "TEST-101",
            "field_language": "n/a",
            "field_name": "course_code",
            "field_value": "TEST-101",
        },
    ]


def test_node_mapper_to_page_profile_row() -> None:
    node = make_node()

    row = MySQLNodeMapper.to_page_profile_row(node)

    expected = MySQLPageProfileMapper.to_row(node.page_profile)  # type: ignore[arg-type]
    assert row == expected
    assert row["short_code"] == "TEST-101"
    assert row["name_en_value"] == "Introduction to Autonomous Systems Design"
    assert row["external_key_en"] == "intro-autonomous-systems-TEST-101"
    assert row["external_url_en"] == "https://edu.epfl.ch/coursebook/en/intro-autonomous-systems-TEST-101"
    assert row["is_visible"] == 1


def test_node_mapper_to_page_profile_row_asserts_when_missing() -> None:
    node = Node(
        key=make_node_key(),
        title="Title",
        text_source="Source",
        raw_text="Raw text",
        page_profile=PageProfile(key=make_node_key()),
    )
    node.page_profile = None

    try:
        MySQLNodeMapper.to_page_profile_row(node)
        assert False, "Expected AssertionError"
    except AssertionError:
        pass


def test_node_mapper_to_simplified_dict() -> None:
    node = make_node()

    data = MySQLNodeMapper.to_simplified_dict(node)

    assert data["institution_id"] == "EPFL"
    assert data["object_type"] == "Course"
    assert data["object_id"] == "TEST-101"
    assert data["object_title"] == "Introduction to Autonomous Systems Design"
    assert data["text_source"] == "course page description"
    assert data["raw_text"] == "Autonomous systems course raw text."

    assert data["custom_fields"] == [
        {
            "field_language": "en",
            "field_name": "summary",
            "field_value": "An introduction to autonomous systems.",
        },
        {
            "field_language": "n/a",
            "field_name": "course_code",
            "field_value": "TEST-101",
        },
    ]

    page_profile = data["page_profile"]

    assert page_profile["short_code"] == "TEST-101"
    assert page_profile["name_en_value"] == "Introduction to Autonomous Systems Design"
    assert page_profile["name_fr_value"] == "Introduction à la conception de systèmes autonomes"

    assert page_profile["description_short_en_value"] == "Learn the fundamentals of autonomous systems."
    assert page_profile["description_medium_en_value"] == "Structured introduction to autonomous systems."
    assert page_profile["description_long_en_value"] == "Detailed long description."

    assert page_profile["external_key_en"] == "intro-autonomous-systems-TEST-101"
    assert page_profile["external_url_en"] == "https://edu.epfl.ch/coursebook/en/intro-autonomous-systems-TEST-101"
    assert page_profile["is_visible"] == 1

    # Optional: explicitly document current mapper behavior
    assert page_profile["name_en_is_auto_generated"] == 0
    assert page_profile["name_en_is_auto_corrected"] == 0
    assert page_profile["name_en_is_auto_translated"] == 0


def test_node_mapper_from_simplified_dict() -> None:
    data = make_simplified_node_dict()

    node = MySQLNodeMapper.from_simplified_dict(data)

    assert isinstance(node, Node)
    assert node.key == NodeKey(
        institution_id="EPFL",
        object_type="Course",
        object_id="TEST-101",
    )
    assert node.title == "Introduction to Autonomous Systems Design"
    assert node.text_source == "course page description"
    assert node.raw_text == "Autonomous systems course raw text."
    assert len(node.field_list.field_list) == 2

    assert node.field_list.field_list[0].key.field_language == "en"
    assert node.field_list.field_list[0].key.field_name == "summary"
    assert node.field_list.field_list[0].field_value == "An introduction to autonomous systems."

    assert node.page_profile is not None
    assert node.page_profile.short_code == "TEST-101"
    assert node.page_profile.name.en.value == "Introduction to Autonomous Systems Design"
    assert node.page_profile.name.fr.value == "Introduction à la conception de systèmes autonomes"
    assert node.page_profile.description.short.en.value == "Learn the fundamentals of autonomous systems."
    assert node.page_profile.description.medium.en.value == "Structured introduction to autonomous systems."
    assert node.page_profile.description.long.en.value == "Detailed long description."
    assert node.page_profile.external_key.en == "intro-autonomous-systems-TEST-101"
    assert node.page_profile.external_url.en == "https://edu.epfl.ch/coursebook/en/intro-autonomous-systems-TEST-101"
    assert node.page_profile.is_visible is True


def test_node_mapper_to_simplified_dict_list() -> None:
    node_1 = make_node()

    data_2 = make_simplified_node_dict()
    data_2["object_id"] = "TEST-102"
    data_2["object_title"] = "Another Course"
    data_2["page_profile"] = {
        "short_code": "TEST-102",
        "name_en_value": "Another Course",
        "is_visible": True,
    }
    node_2 = MySQLNodeMapper.from_simplified_dict(data_2)

    out = MySQLNodeMapper.to_simplified_dict_list([node_1, node_2])

    assert len(out) == 2
    assert out[0]["object_id"] == "TEST-101"
    assert out[1]["object_id"] == "TEST-102"
    assert out[0]["page_profile"]["short_code"] == "TEST-101"
    assert out[1]["page_profile"]["short_code"] == "TEST-102"


def test_node_mapper_from_simplified_dict_list() -> None:
    data_1 = make_simplified_node_dict()

    data_2 = make_simplified_node_dict()
    data_2["object_id"] = "TEST-102"
    data_2["object_title"] = "Another Course"
    data_2["page_profile"] = {
        "short_code": "TEST-102",
        "name_en_value": "Another Course",
        "is_visible": True,
    }

    nodes = MySQLNodeMapper.from_simplified_dict_list([data_1, data_2])

    assert len(nodes) == 2
    assert isinstance(nodes[0], Node)
    assert isinstance(nodes[1], Node)

    assert nodes[0].key.object_id == "TEST-101"
    assert nodes[1].key.object_id == "TEST-102"
    assert nodes[0].title == "Introduction to Autonomous Systems Design"
    assert nodes[1].title == "Another Course"

    assert nodes[0].page_profile is not None
    assert nodes[1].page_profile is not None
    assert nodes[0].page_profile.short_code == "TEST-101"
    assert nodes[1].page_profile.short_code == "TEST-102"


def test_node_mapper_from_simplified_dict_list_with_none() -> None:
    nodes = MySQLNodeMapper.from_simplified_dict_list(None)

    assert nodes == []
