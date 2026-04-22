# tests/unit_tests/domain/models/test_mdl_edge.py
from __future__ import annotations

from typing import Any

from graphregistry.domain.models.entities.mdl_base import EdgeFieldKey, EdgeKey
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeField, EdgeFieldList, EdgeList


def make_edge_key(
    from_institution_id: str = "EPFL",
    from_object_type: str = "Course",
    from_object_id: str = "CS-101",
    to_institution_id: str = "EPFL",
    to_object_type: str = "Person",
    to_object_id: str = "12345",
    context: str = "teacher",
) -> EdgeKey:
    return EdgeKey(
        from_institution_id=from_institution_id,
        from_object_type=from_object_type,
        from_object_id=from_object_id,
        to_institution_id=to_institution_id,
        to_object_type=to_object_type,
        to_object_id=to_object_id,
        context=context,
    )


def make_edge_field(
    key: EdgeKey | None = None,
    field_language: str = "en",
    field_name: str = "role",
    field_value: Any = "Lecturer",
) -> EdgeField:
    key = key or make_edge_key()
    return EdgeField(
        key=EdgeFieldKey(
            key=key,
            field_language=field_language,
            field_name=field_name,
        ),
        field_value=field_value,
    )


class TestEdgeField:
    def test_from_json(self) -> None:
        edge_key = make_edge_key()

        field = EdgeField.from_json(
            {
                "field_language": "fr",
                "field_name": "role",
                "field_value": "Enseignant",
            },
            edge_key=edge_key,
        )

        assert field.key.key == edge_key
        assert field.key.field_language == "fr"
        assert field.key.field_name == "role"
        assert field.field_value == "Enseignant"

    def test_from_json_defaults_missing_field_value_to_empty_string(self) -> None:
        edge_key = make_edge_key()

        field = EdgeField.from_json(
            {
                "field_language": "en",
                "field_name": "role",
            },
            edge_key=edge_key,
        )

        assert field.field_value == ""

    def test_from_dict(self) -> None:
        field = EdgeField.from_dict(
            {
                "from_institution_id": "EPFL",
                "from_object_type": "Course",
                "from_object_id": "CS-101",
                "to_institution_id": "EPFL",
                "to_object_type": "Person",
                "to_object_id": "12345",
                "context": "teacher",
                "field_language": "en",
                "field_name": "role",
                "field_value": "Lecturer",
            }
        )

        assert field.key.key == make_edge_key()
        assert field.key.field_language == "en"
        assert field.key.field_name == "role"
        assert field.field_value == "Lecturer"

    def test_from_dict_defaults_missing_field_values(self) -> None:
        field = EdgeField.from_dict(
            {
                "from_institution_id": "EPFL",
                "from_object_type": "Course",
                "from_object_id": "CS-101",
                "to_institution_id": "EPFL",
                "to_object_type": "Person",
                "to_object_id": "12345",
                "context": "teacher",
            }
        )

        assert field.key.field_language == ""
        assert field.key.field_name == ""
        assert field.field_value == ""

    def test_set_from_json_updates_existing_field(self) -> None:
        field = make_edge_field(field_language="en", field_name="role", field_value="Lecturer")

        field.set_from_json(
            {
                "field_language": "de",
                "field_name": "rolle",
                "field_value": "Dozent",
            }
        )

        assert field.key.field_language == "de"
        assert field.key.field_name == "rolle"
        assert field.field_value == "Dozent"

    def test_set_from_json_keeps_existing_values_when_missing(self) -> None:
        field = make_edge_field(field_language="en", field_name="role", field_value="Lecturer")

        field.set_from_json({})

        assert field.key.field_language == "en"
        assert field.key.field_name == "role"
        assert field.field_value == "Lecturer"

    def test_to_json(self) -> None:
        field = make_edge_field(field_language="it", field_name="ruolo", field_value="Docente")

        data = field.to_json()

        assert data["key"]["key"]["from_institution_id"] == "EPFL"
        assert data["key"]["key"]["from_object_type"] == "Course"
        assert data["key"]["key"]["to_object_type"] == "Person"
        assert data["key"]["field_language"] == "it"
        assert data["key"]["field_name"] == "ruolo"
        assert data["field_value"] == "Docente"

    def test_to_dict(self) -> None:
        field = make_edge_field(field_language="it", field_name="ruolo", field_value="Docente")

        data = field.to_dict()

        assert data == {
            "from_institution_id": "EPFL",
            "from_object_type": "Course",
            "from_object_id": "CS-101",
            "to_institution_id": "EPFL",
            "to_object_type": "Person",
            "to_object_id": "12345",
            "context": "teacher",
            "field_language": "it",
            "field_name": "ruolo",
            "field_value": "Docente",
        }


class TestEdgeFieldList:
    def test_from_json(self) -> None:
        key = make_edge_key()

        field_list = EdgeFieldList.from_json(
            [
                {"field_language": "en", "field_name": "role", "field_value": "Lecturer"},
                {"field_language": "fr", "field_name": "role", "field_value": "Enseignant"},
            ],
            edge_key=key,
        )

        assert len(field_list) == 2
        assert field_list.field_list[0].key.key == key
        assert field_list.field_list[1].field_value == "Enseignant"

    def test_from_dicts(self) -> None:
        field_list = EdgeFieldList.from_dicts(
            [
                {
                    "from_institution_id": "EPFL",
                    "from_object_type": "Course",
                    "from_object_id": "CS-101",
                    "to_institution_id": "EPFL",
                    "to_object_type": "Person",
                    "to_object_id": "12345",
                    "context": "teacher",
                    "field_language": "en",
                    "field_name": "role",
                    "field_value": "Lecturer",
                },
                {
                    "from_institution_id": "EPFL",
                    "from_object_type": "Course",
                    "from_object_id": "CS-101",
                    "to_institution_id": "EPFL",
                    "to_object_type": "Person",
                    "to_object_id": "12345",
                    "context": "teacher",
                    "field_language": "fr",
                    "field_name": "role",
                    "field_value": "Enseignant",
                },
            ]
        )

        assert len(field_list) == 2
        assert field_list.field_list[0].key.field_language == "en"
        assert field_list.field_list[1].field_value == "Enseignant"

    def test_set_from_list(self) -> None:
        key = make_edge_key()
        field_list = EdgeFieldList(field_list=[make_edge_field(field_name="old")])

        field_list.set_from_list(
            [
                {"field_language": "en", "field_name": "new", "field_value": "X"},
            ],
            edge_key=key,
        )

        assert len(field_list) == 1
        assert field_list.field_list[0].key.field_name == "new"
        assert field_list.field_list[0].field_value == "X"

    def test_append(self) -> None:
        field_list = EdgeFieldList()
        field = make_edge_field()

        field_list.append(field)

        assert field_list.field_list == [field]

    def test_extend(self) -> None:
        field_list = EdgeFieldList()
        fields = [make_edge_field(field_name="a"), make_edge_field(field_name="b")]

        field_list.extend(fields)

        assert field_list.field_list == fields

    def test_get(self) -> None:
        f1 = make_edge_field(field_language="en", field_name="role", field_value="Lecturer")
        f2 = make_edge_field(field_language="fr", field_name="role", field_value="Enseignant")
        field_list = EdgeFieldList(field_list=[f1, f2])

        assert field_list.get("en", "role") == f1
        assert field_list.get("fr", "role") == f2
        assert field_list.get("de", "role") is None

    def test_exists(self) -> None:
        field_list = EdgeFieldList(
            field_list=[make_edge_field(field_language="en", field_name="role")]
        )

        assert field_list.exists("en", "role") is True
        assert field_list.exists("fr", "role") is False

    def test_to_json(self) -> None:
        field_list = EdgeFieldList(
            field_list=[
                make_edge_field(field_language="en", field_name="role", field_value="Lecturer"),
                make_edge_field(field_language="fr", field_name="role", field_value="Enseignant"),
            ]
        )

        data = field_list.to_json()

        assert len(data) == 2
        assert data[0]["field_value"] == "Lecturer"
        assert data[1]["key"]["field_language"] == "fr"

    def test_to_list(self) -> None:
        field_list = EdgeFieldList(
            field_list=[
                make_edge_field(field_language="en", field_name="role", field_value="Lecturer"),
                make_edge_field(field_language="fr", field_name="role", field_value="Enseignant"),
            ]
        )

        data = field_list.to_list()

        assert data == [
            {
                "from_institution_id": "EPFL",
                "from_object_type": "Course",
                "from_object_id": "CS-101",
                "to_institution_id": "EPFL",
                "to_object_type": "Person",
                "to_object_id": "12345",
                "context": "teacher",
                "field_language": "en",
                "field_name": "role",
                "field_value": "Lecturer",
            },
            {
                "from_institution_id": "EPFL",
                "from_object_type": "Course",
                "from_object_id": "CS-101",
                "to_institution_id": "EPFL",
                "to_object_type": "Person",
                "to_object_id": "12345",
                "context": "teacher",
                "field_language": "fr",
                "field_name": "role",
                "field_value": "Enseignant",
            },
        ]

    def test_iter_fields(self) -> None:
        f1 = make_edge_field(field_name="a")
        f2 = make_edge_field(field_name="b")
        field_list = EdgeFieldList(field_list=[f1, f2])

        out = list(field_list.iter_fields())

        assert out == [f1, f2]

    def test_len(self) -> None:
        field_list = EdgeFieldList(field_list=[make_edge_field(), make_edge_field(field_name="x")])
        assert len(field_list) == 2

    def test_bool(self) -> None:
        assert bool(EdgeFieldList()) is False
        assert bool(EdgeFieldList(field_list=[make_edge_field()])) is True


class TestEdge:
    def test_from_json(self) -> None:
        key = make_edge_key()
        data = {
            "key": key.model_dump(mode="json"),
            "field_list": {
                "field_list": [
                    {
                        "key": {
                            "key": key.model_dump(mode="json"),
                            "field_language": "en",
                            "field_name": "role",
                        },
                        "field_value": "Lecturer",
                    }
                ]
            },
        }

        edge = Edge.from_json(data)

        assert edge.key == key
        assert len(edge.field_list) == 1
        assert edge.field_list.field_list[0].field_value == "Lecturer"

    def test_from_dict(self) -> None:
        edge = Edge.from_dict(
            {
                "from_institution_id": "EPFL",
                "from_object_type": "Course",
                "from_object_id": "CS-101",
                "to_institution_id": "EPFL",
                "to_object_type": "Person",
                "to_object_id": "12345",
                "context": "teacher",
                "field_list": [
                    {
                        "field_language": "en",
                        "field_name": "role",
                        "field_value": "Lecturer",
                    },
                    {
                        "field_language": "fr",
                        "field_name": "role",
                        "field_value": "Enseignant",
                    },
                ],
            }
        )

        assert edge.key == make_edge_key()
        assert len(edge.field_list) == 2
        assert edge.field_list.field_list[0].key.key == make_edge_key()
        assert edge.field_list.field_list[1].field_value == "Enseignant"

    def test_from_dict_defaults_missing_field_list_to_empty(self) -> None:
        edge = Edge.from_dict(
            {
                "from_institution_id": "EPFL",
                "from_object_type": "Course",
                "from_object_id": "CS-101",
                "to_institution_id": "EPFL",
                "to_object_type": "Person",
                "to_object_id": "12345",
                "context": "teacher",
            }
        )

        assert edge.key == make_edge_key()
        assert len(edge.field_list) == 0

    def test_to_json(self) -> None:
        edge = Edge(
            key=make_edge_key(),
            field_list=EdgeFieldList(
                field_list=[make_edge_field(field_language="en", field_name="role", field_value="Lecturer")]
            ),
        )

        data = edge.to_json()

        assert data["key"]["from_institution_id"] == "EPFL"
        assert data["key"]["to_object_type"] == "Person"
        assert data["field_list"]["field_list"][0]["field_value"] == "Lecturer"

    def test_to_dict(self) -> None:
        edge = Edge(
            key=make_edge_key(),
            field_list=EdgeFieldList(
                field_list=[
                    make_edge_field(field_language="en", field_name="role", field_value="Lecturer"),
                    make_edge_field(field_language="fr", field_name="role", field_value="Enseignant"),
                ]
            ),
        )

        data = edge.to_dict()

        assert data == {
            "from_institution_id": "EPFL",
            "from_object_type": "Course",
            "from_object_id": "CS-101",
            "to_institution_id": "EPFL",
            "to_object_type": "Person",
            "to_object_id": "12345",
            "context": "teacher",
            "field_list": [
                {
                    "from_institution_id": "EPFL",
                    "from_object_type": "Course",
                    "from_object_id": "CS-101",
                    "to_institution_id": "EPFL",
                    "to_object_type": "Person",
                    "to_object_id": "12345",
                    "context": "teacher",
                    "field_language": "en",
                    "field_name": "role",
                    "field_value": "Lecturer",
                },
                {
                    "from_institution_id": "EPFL",
                    "from_object_type": "Course",
                    "from_object_id": "CS-101",
                    "to_institution_id": "EPFL",
                    "to_object_type": "Person",
                    "to_object_id": "12345",
                    "context": "teacher",
                    "field_language": "fr",
                    "field_name": "role",
                    "field_value": "Enseignant",
                },
            ],
        }

    def test_get_field(self) -> None:
        f1 = make_edge_field(field_language="en", field_name="role", field_value="Lecturer")
        f2 = make_edge_field(field_language="fr", field_name="role", field_value="Enseignant")
        edge = Edge(
            key=make_edge_key(),
            field_list=EdgeFieldList(field_list=[f1, f2]),
        )

        assert edge.get_field("en", "role") == f1
        assert edge.get_field("fr", "role") == f2
        assert edge.get_field("de", "role") is None

    def test_has_field(self) -> None:
        edge = Edge(
            key=make_edge_key(),
            field_list=EdgeFieldList(
                field_list=[make_edge_field(field_language="en", field_name="role")]
            ),
        )

        assert edge.has_field("en", "role") is True
        assert edge.has_field("fr", "role") is False


class TestEdgeList:
    def test_from_json(self) -> None:
        key1 = make_edge_key(from_object_id="A", to_object_id="1")
        key2 = make_edge_key(from_object_id="B", to_object_id="2")

        data = [
            {
                "key": key1.model_dump(mode="json"),
                "field_list": {"field_list": []},
            },
            {
                "key": key2.model_dump(mode="json"),
                "field_list": {"field_list": []},
            },
        ]

        edge_list = EdgeList.from_json(data)

        assert len(edge_list) == 2
        assert edge_list.edge_list[0].key == key1
        assert edge_list.edge_list[1].key == key2

    def test_from_dicts(self) -> None:
        data = [
            {
                "from_institution_id": "EPFL",
                "from_object_type": "Course",
                "from_object_id": "A",
                "to_institution_id": "EPFL",
                "to_object_type": "Person",
                "to_object_id": "1",
                "context": "teacher",
                "field_list": [
                    {"field_language": "en", "field_name": "role", "field_value": "Lecturer"},
                ],
            },
            {
                "from_institution_id": "EPFL",
                "from_object_type": "Course",
                "from_object_id": "B",
                "to_institution_id": "EPFL",
                "to_object_type": "Person",
                "to_object_id": "2",
                "context": "assistant",
                "field_list": [
                    {"field_language": "en", "field_name": "role", "field_value": "TA"},
                ],
            },
        ]

        edge_list = EdgeList.from_dicts(data)

        assert len(edge_list) == 2
        assert edge_list.edge_list[0].key.from_object_id == "A"
        assert edge_list.edge_list[0].field_list.field_list[0].field_value == "Lecturer"
        assert edge_list.edge_list[1].key.context == "assistant"
        assert edge_list.edge_list[1].field_list.field_list[0].field_value == "TA"

    def test_append(self) -> None:
        edge_list = EdgeList()
        edge = Edge(key=make_edge_key())

        edge_list.append(edge)

        assert edge_list.edge_list == [edge]

    def test_extend(self) -> None:
        e1 = Edge(key=make_edge_key(from_object_id="A", to_object_id="1"))
        e2 = Edge(key=make_edge_key(from_object_id="B", to_object_id="2"))
        edge_list = EdgeList()

        edge_list.extend([e1, e2])

        assert edge_list.edge_list == [e1, e2]

    def test_to_json(self) -> None:
        e1 = Edge(key=make_edge_key(from_object_id="A", to_object_id="1"))
        e2 = Edge(key=make_edge_key(from_object_id="B", to_object_id="2"))
        edge_list = EdgeList(edge_list=[e1, e2])

        data = edge_list.to_json()

        assert len(data) == 2
        assert data[0]["key"]["from_object_id"] == "A"
        assert data[1]["key"]["from_object_id"] == "B"

    def test_to_list(self) -> None:
        e1 = Edge(
            key=make_edge_key(from_object_id="A", to_object_id="1"),
            field_list=EdgeFieldList(
                field_list=[make_edge_field(key=make_edge_key(from_object_id="A", to_object_id="1"))]
            ),
        )
        e2 = Edge(
            key=make_edge_key(from_object_id="B", to_object_id="2"),
            field_list=EdgeFieldList(),
        )
        edge_list = EdgeList(edge_list=[e1, e2])

        data = edge_list.to_list()

        assert len(data) == 2
        assert data[0]["from_object_id"] == "A"
        assert data[0]["field_list"][0]["field_name"] == "role"
        assert data[1]["from_object_id"] == "B"
        assert data[1]["field_list"] == []

    def test_iter_edges(self) -> None:
        e1 = Edge(key=make_edge_key(from_object_id="A", to_object_id="1"))
        e2 = Edge(key=make_edge_key(from_object_id="B", to_object_id="2"))
        edge_list = EdgeList(edge_list=[e1, e2])

        out = list(edge_list.iter_edges())

        assert out == [e1, e2]

    def test_len(self) -> None:
        edge_list = EdgeList(edge_list=[Edge(key=make_edge_key())])
        assert len(edge_list) == 1

    def test_bool(self) -> None:
        assert bool(EdgeList()) is False
        assert bool(EdgeList(edge_list=[Edge(key=make_edge_key())])) is True
