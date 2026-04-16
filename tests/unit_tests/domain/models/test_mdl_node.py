# tests/unit_tests/domain/models/test_mdl_node.py
from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from graphregistry.domain.models.mdl_base import NodeFieldKey, NodeKey
from graphregistry.domain.models.mdl_node import Node, NodeField, NodeFieldList, NodeList
from graphregistry.domain.models.mdl_pageprofile import PageProfile


def make_node_key(
    institution_id: str = "EPFL",
    object_type: str = "Course",
    object_id: str = "CS-101",
) -> NodeKey:
    return NodeKey(
        institution_id=institution_id,
        object_type=object_type,
        object_id=object_id,
    )


def make_node_field(
    key: NodeKey | None = None,
    field_language: str = "en",
    field_name: str = "summary",
    field_value: Any = "hello",
) -> NodeField:
    key = key or make_node_key()
    return NodeField(
        key=NodeFieldKey(
            key=key,
            field_language=field_language,
            field_name=field_name,
        ),
        field_value=field_value,
    )


class TestNodeField:
    def test_validate_key_consistency_accepts_node_field_key(self) -> None:
        field = make_node_field()
        assert isinstance(field.key, NodeFieldKey)

    def test_validate_key_consistency_rejects_invalid_key_type(self) -> None:
        with pytest.raises(ValidationError):
            NodeField.model_validate(
                {
                    "key": "not-a-node-field-key",
                    "field_value": "x",
                }
            )

    def test_from_json(self) -> None:
        node_key = make_node_key()
        field = NodeField.from_json(
            {
                "field_language": "fr",
                "field_name": "title",
                "field_value": "Bonjour",
            },
            node_key=node_key,
        )

        assert field.key.key == node_key
        assert field.key.field_language == "fr"
        assert field.key.field_name == "title"
        assert field.field_value == "Bonjour"

    def test_set_from_json_updates_existing_field(self) -> None:
        field = make_node_field(field_language="en", field_name="summary", field_value="old")

        field.set_from_json(
            {
                "field_language": "de",
                "field_name": "beschreibung",
                "field_value": "neu",
            }
        )

        assert field.key.field_language == "de"
        assert field.key.field_name == "beschreibung"
        assert field.field_value == "neu"

    def test_set_from_json_keeps_existing_values_when_missing(self) -> None:
        field = make_node_field(field_language="en", field_name="summary", field_value="old")

        field.set_from_json({})

        assert field.key.field_language == "en"
        assert field.key.field_name == "summary"
        assert field.field_value == "old"

    def test_to_json(self) -> None:
        field = make_node_field(field_language="it", field_name="label", field_value="ciao")
        data = field.to_json()

        assert data["key"]["key"]["institution_id"] == "EPFL"
        assert data["key"]["field_language"] == "it"
        assert data["key"]["field_name"] == "label"
        assert data["field_value"] == "ciao"

    def test_to_dict(self) -> None:
        field = make_node_field(field_language="it", field_name="label", field_value="ciao")
        data = field.to_dict()

        assert data == {
            "institution_id": "EPFL",
            "object_type": "Course",
            "object_id": "CS-101",
            "field_language": "it",
            "field_name": "label",
            "field_value": "ciao",
        }

    def test_matches_with_name_only(self) -> None:
        field = make_node_field(field_language="en", field_name="summary")
        assert field.matches("summary") is True
        assert field.matches("title") is False

    def test_matches_with_name_and_language(self) -> None:
        field = make_node_field(field_language="fr", field_name="summary")
        assert field.matches("summary", "fr") is True
        assert field.matches("summary", "en") is False


class TestNodeFieldList:
    def test_from_json(self) -> None:
        key = make_node_key()
        field_list = NodeFieldList.from_json(
            [
                {"field_language": "en", "field_name": "summary", "field_value": "A"},
                {"field_language": "fr", "field_name": "summary", "field_value": "B"},
            ],
            key=key,
        )

        assert len(field_list) == 2
        assert field_list.field_list[0].key.key == key
        assert field_list.field_list[1].field_value == "B"

    def test_set_from_list(self) -> None:
        key = make_node_key()
        field_list = NodeFieldList(field_list=[make_node_field(field_name="old")])

        field_list.set_from_list(
            [
                {"field_language": "en", "field_name": "new", "field_value": "X"},
            ],
            node_key=key,
        )

        assert len(field_list) == 1
        assert field_list.field_list[0].key.field_name == "new"
        assert field_list.field_list[0].field_value == "X"

    def test_to_json(self) -> None:
        field_list = NodeFieldList(
            field_list=[
                make_node_field(field_language="en", field_name="summary", field_value="A"),
                make_node_field(field_language="fr", field_name="summary", field_value="B"),
            ]
        )

        data = field_list.to_json()

        assert len(data) == 2
        assert data[0]["field_value"] == "A"
        assert data[1]["key"]["field_language"] == "fr"

    def test_to_list(self) -> None:
        field_list = NodeFieldList(
            field_list=[
                make_node_field(field_language="en", field_name="summary", field_value="A"),
                make_node_field(field_language="fr", field_name="summary", field_value="B"),
            ]
        )

        data = field_list.to_list()

        assert data == [
            {
                "institution_id": "EPFL",
                "object_type": "Course",
                "object_id": "CS-101",
                "field_language": "en",
                "field_name": "summary",
                "field_value": "A",
            },
            {
                "institution_id": "EPFL",
                "object_type": "Course",
                "object_id": "CS-101",
                "field_language": "fr",
                "field_name": "summary",
                "field_value": "B",
            },
        ]

    def test_iter_fields(self) -> None:
        f1 = make_node_field(field_name="a")
        f2 = make_node_field(field_name="b")
        field_list = NodeFieldList(field_list=[f1, f2])

        out = list(field_list.iter_fields())

        assert out == [f1, f2]

    def test_len(self) -> None:
        field_list = NodeFieldList(field_list=[make_node_field(), make_node_field(field_name="x")])
        assert len(field_list) == 2

    def test_bool(self) -> None:
        assert bool(NodeFieldList()) is False
        assert bool(NodeFieldList(field_list=[make_node_field()])) is True

    def test_append(self) -> None:
        field_list = NodeFieldList()
        field = make_node_field()

        field_list.append(field)

        assert field_list.field_list == [field]

    def test_extend(self) -> None:
        field_list = NodeFieldList()
        fields = [make_node_field(field_name="a"), make_node_field(field_name="b")]

        field_list.extend(fields)

        assert field_list.field_list == fields

    def test_get_with_name_only(self) -> None:
        f1 = make_node_field(field_language="en", field_name="summary", field_value="EN")
        f2 = make_node_field(field_language="fr", field_name="summary", field_value="FR")
        field_list = NodeFieldList(field_list=[f1, f2])

        out = field_list.get("summary")

        assert out == f1

    def test_get_with_name_and_language(self) -> None:
        f1 = make_node_field(field_language="en", field_name="summary", field_value="EN")
        f2 = make_node_field(field_language="fr", field_name="summary", field_value="FR")
        field_list = NodeFieldList(field_list=[f1, f2])

        out = field_list.get("summary", "fr")

        assert out == f2

    def test_get_returns_none_when_missing(self) -> None:
        field_list = NodeFieldList(field_list=[make_node_field(field_name="summary")])
        assert field_list.get("missing") is None

    def test_get_value_returns_field_value(self) -> None:
        field_list = NodeFieldList(field_list=[make_node_field(field_name="summary", field_value="VALUE")])
        assert field_list.get_value("summary") == "VALUE"

    def test_get_value_returns_default(self) -> None:
        field_list = NodeFieldList()
        assert field_list.get_value("missing", default="DEFAULT") == "DEFAULT"

    def test_filter_without_filters_returns_all(self) -> None:
        f1 = make_node_field(field_language="en", field_name="summary")
        f2 = make_node_field(field_language="fr", field_name="title")
        field_list = NodeFieldList(field_list=[f1, f2])

        out = field_list.filter()

        assert out == [f1, f2]

    def test_filter_by_name(self) -> None:
        f1 = make_node_field(field_language="en", field_name="summary")
        f2 = make_node_field(field_language="fr", field_name="title")
        field_list = NodeFieldList(field_list=[f1, f2])

        out = field_list.filter(field_name="summary")

        assert out == [f1]

    def test_filter_by_language(self) -> None:
        f1 = make_node_field(field_language="en", field_name="summary")
        f2 = make_node_field(field_language="fr", field_name="title")
        field_list = NodeFieldList(field_list=[f1, f2])

        out = field_list.filter(field_language="fr")

        assert out == [f2]

    def test_filter_by_name_and_language(self) -> None:
        f1 = make_node_field(field_language="en", field_name="summary", field_value="EN")
        f2 = make_node_field(field_language="fr", field_name="summary", field_value="FR")
        f3 = make_node_field(field_language="fr", field_name="title", field_value="TITLE")
        field_list = NodeFieldList(field_list=[f1, f2, f3])

        out = field_list.filter(field_name="summary", field_language="fr")

        assert out == [f2]

    def test_upsert_replaces_existing(self) -> None:
        key = make_node_key()
        original = make_node_field(key=key, field_language="en", field_name="summary", field_value="old")
        updated = make_node_field(key=key, field_language="en", field_name="summary", field_value="new")
        field_list = NodeFieldList(field_list=[original])

        field_list.upsert(updated)

        assert len(field_list) == 1
        assert field_list.field_list[0] == updated

    def test_upsert_appends_when_missing(self) -> None:
        field_list = NodeFieldList(field_list=[make_node_field(field_name="summary")])
        new_field = make_node_field(field_name="title")

        field_list.upsert(new_field)

        assert len(field_list) == 2
        assert field_list.field_list[-1] == new_field

    def test_remove_by_name_only(self) -> None:
        f1 = make_node_field(field_language="en", field_name="summary")
        f2 = make_node_field(field_language="fr", field_name="summary")
        f3 = make_node_field(field_language="en", field_name="title")
        field_list = NodeFieldList(field_list=[f1, f2, f3])

        removed = field_list.remove("summary")

        assert removed == 2
        assert field_list.field_list == [f3]

    def test_remove_by_name_and_language(self) -> None:
        f1 = make_node_field(field_language="en", field_name="summary")
        f2 = make_node_field(field_language="fr", field_name="summary")
        field_list = NodeFieldList(field_list=[f1, f2])

        removed = field_list.remove("summary", "fr")

        assert removed == 1
        assert field_list.field_list == [f1]

    def test_remove_returns_zero_when_nothing_removed(self) -> None:
        field_list = NodeFieldList(field_list=[make_node_field(field_name="summary")])

        removed = field_list.remove("missing")

        assert removed == 0
        assert len(field_list) == 1


class TestNode:
    def test_set_default_page_profile_when_missing(self) -> None:
        key = make_node_key()
        node = Node(key=key)

        assert node.page_profile is not None
        assert isinstance(node.page_profile, PageProfile)
        assert node.page_profile.key == key

    def test_set_default_page_profile_rewrites_mismatched_page_profile_key(self) -> None:
        node_key = make_node_key()
        other_key = make_node_key(object_id="OTHER")
        node = Node(
            key=node_key,
            page_profile=PageProfile(key=other_key, short_code="SC"),
        )

        assert node.page_profile is not None
        assert node.page_profile.key == node_key
        assert node.page_profile.short_code == "SC"

    def test_validate_field_keys_rewrites_field_keys_to_node_key(self) -> None:
        node_key = make_node_key()
        other_key = make_node_key(object_id="OTHER")
        mismatched_field = make_node_field(key=other_key, field_name="summary", field_value="X")

        node = Node(
            key=node_key,
            field_list=NodeFieldList(field_list=[mismatched_field]),
        )

        assert node.field_list.field_list[0].key.key == node_key
        assert node.field_list.field_list[0].field_value == "X"

    def test_from_json(self) -> None:
        key = make_node_key()
        data = {
            "key": key.model_dump(mode="json"),
            "title": "Intro",
            "text_source": "catalog",
            "raw_text": "Long text",
            "field_list": {
                "field_list": [
                    {
                        "key": {
                            "key": key.model_dump(mode="json"),
                            "field_language": "en",
                            "field_name": "summary",
                        },
                        "field_value": "Hello",
                    }
                ]
            },
            "page_profile": {
                "key": key.model_dump(mode="json"),
                "short_code": "CS101",
            },
        }

        node = Node.from_json(data)

        assert node.key == key
        assert node.title == "Intro"
        assert node.text_source == "catalog"
        assert node.raw_text == "Long text"
        assert len(node.field_list) == 1
        assert node.page_profile is not None
        assert node.page_profile.short_code == "CS101"

    def test_to_json(self) -> None:
        node = Node(
            key=make_node_key(),
            title="Intro",
            text_source="catalog",
            raw_text="Long text",
            field_list=NodeFieldList(field_list=[make_node_field(field_name="summary", field_value="Hello")]),
        )

        data = node.to_json()

        assert data["title"] == "Intro"
        assert data["text_source"] == "catalog"
        assert data["raw_text"] == "Long text"
        assert data["field_list"]["field_list"][0]["field_value"] == "Hello"
        assert data["page_profile"]["key"]["object_id"] == "CS-101"

    def test_has_field(self) -> None:
        node = Node(
            key=make_node_key(),
            field_list=NodeFieldList(
                field_list=[make_node_field(field_language="en", field_name="summary")]
            ),
        )

        assert node.has_field("summary") is True
        assert node.has_field("summary", "en") is True
        assert node.has_field("summary", "fr") is False
        assert node.has_field("missing") is False

    def test_get_field(self) -> None:
        field = make_node_field(field_language="fr", field_name="summary", field_value="Bonjour")
        node = Node(
            key=make_node_key(),
            field_list=NodeFieldList(field_list=[field]),
        )

        assert node.get_field("summary", "fr") == field
        assert node.get_field("summary", "en") is None

    def test_get_field_value(self) -> None:
        node = Node(
            key=make_node_key(),
            field_list=NodeFieldList(
                field_list=[make_node_field(field_language="en", field_name="summary", field_value="VALUE")]
            ),
        )

        assert node.get_field_value("summary", "en") == "VALUE"
        assert node.get_field_value("missing", default="DEFAULT") == "DEFAULT"

    def test_set_field_value_updates_existing(self) -> None:
        node = Node(
            key=make_node_key(),
            field_list=NodeFieldList(
                field_list=[make_node_field(field_language="en", field_name="summary", field_value="OLD")]
            ),
        )

        node.set_field_value("summary", "NEW", field_language="en")

        assert len(node.field_list) == 1
        assert node.get_field_value("summary", "en") == "NEW"

    def test_set_field_value_appends_new(self) -> None:
        node = Node(key=make_node_key())

        node.set_field_value("summary", "VALUE", field_language="en")

        assert len(node.field_list) == 1
        assert node.get_field_value("summary", "en") == "VALUE"

    def test_remove_field(self) -> None:
        node = Node(
            key=make_node_key(),
            field_list=NodeFieldList(
                field_list=[
                    make_node_field(field_language="en", field_name="summary"),
                    make_node_field(field_language="fr", field_name="summary"),
                    make_node_field(field_language="en", field_name="title"),
                ]
            ),
        )

        removed = node.remove_field("summary", "fr")

        assert removed == 1
        assert node.has_field("summary", "en") is True
        assert node.has_field("summary", "fr") is False
        assert node.has_field("title", "en") is True

    def test_iter_fields(self) -> None:
        f1 = make_node_field(field_name="a")
        f2 = make_node_field(field_name="b")
        node = Node(
            key=make_node_key(),
            field_list=NodeFieldList(field_list=[f1, f2]),
        )

        out = list(node.iter_fields())

        assert out == [f1, f2]


class TestNodeList:
    def test_from_json(self) -> None:
        key1 = make_node_key(object_id="A")
        key2 = make_node_key(object_id="B")

        data = [
            {
                "key": key1.model_dump(mode="json"),
                "title": "Node A",
                "field_list": {"field_list": []},
                "page_profile": {"key": key1.model_dump(mode="json")},
            },
            {
                "key": key2.model_dump(mode="json"),
                "title": "Node B",
                "field_list": {"field_list": []},
                "page_profile": {"key": key2.model_dump(mode="json")},
            },
        ]

        node_list = NodeList.from_json(data)

        assert len(node_list) == 2
        assert node_list.node_list[0].key == key1
        assert node_list.node_list[1].title == "Node B"

    def test_to_json(self) -> None:
        n1 = Node(key=make_node_key(object_id="A"), title="Node A")
        n2 = Node(key=make_node_key(object_id="B"), title="Node B")
        node_list = NodeList(node_list=[n1, n2])

        data = node_list.to_json()

        assert len(data) == 2
        assert data[0]["title"] == "Node A"
        assert data[1]["title"] == "Node B"

    def test_iter_nodes(self) -> None:
        n1 = Node(key=make_node_key(object_id="A"))
        n2 = Node(key=make_node_key(object_id="B"))
        node_list = NodeList(node_list=[n1, n2])

        out = list(node_list.iter_nodes())

        assert out == [n1, n2]

    def test_len(self) -> None:
        node_list = NodeList(node_list=[Node(key=make_node_key(object_id="A"))])
        assert len(node_list) == 1

    def test_bool(self) -> None:
        assert bool(NodeList()) is False
        assert bool(NodeList(node_list=[Node(key=make_node_key())])) is True

    def test_append(self) -> None:
        node_list = NodeList()
        node = Node(key=make_node_key())

        node_list.append(node)

        assert node_list.node_list == [node]

    def test_extend(self) -> None:
        n1 = Node(key=make_node_key(object_id="A"))
        n2 = Node(key=make_node_key(object_id="B"))
        node_list = NodeList()

        node_list.extend([n1, n2])

        assert node_list.node_list == [n1, n2]

    def test_get(self) -> None:
        key1 = make_node_key(object_id="A")
        key2 = make_node_key(object_id="B")
        n1 = Node(key=key1)
        n2 = Node(key=key2)
        node_list = NodeList(node_list=[n1, n2])

        assert node_list.get(key1) == n1
        assert node_list.get(key2) == n2
        assert node_list.get(make_node_key(object_id="C")) is None

    def test_keys(self) -> None:
        key1 = make_node_key(object_id="A")
        key2 = make_node_key(object_id="B")
        node_list = NodeList(
            node_list=[
                Node(key=key1),
                Node(key=key2),
            ]
        )

        assert node_list.keys() == [key1, key2]
