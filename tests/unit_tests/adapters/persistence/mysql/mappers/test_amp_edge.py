# tests/unit_tests/adapters/persistence/mysql/mappers/test_amp_edge.py
from __future__ import annotations

from graphregistry.adapters.persistence.mysql.mappers.amp_edge import (
    MySQLEdgeFieldMapper,
    MySQLEdgeMapper,
)
from graphregistry.domain.models.mdl_base import EdgeFieldKey, EdgeKey
from graphregistry.domain.models.mdl_edge import Edge, EdgeField, EdgeFieldList


def make_edge_key() -> EdgeKey:
    return EdgeKey(
        from_institution_id="EPFL",
        from_object_type="Course",
        from_object_id="TEST-101",
        to_institution_id="EPFL",
        to_object_type="Person",
        to_object_id="123456",
        context="teacher",
    )


def make_edge_field_1(edge_key: EdgeKey | None = None) -> EdgeField:
    edge_key = edge_key or make_edge_key()
    return EdgeField(
        key=EdgeFieldKey(
            key=edge_key,
            field_language="en",
            field_name="role",
        ),
        field_value="Lecturer",
    )


def make_edge_field_2(edge_key: EdgeKey | None = None) -> EdgeField:
    edge_key = edge_key or make_edge_key()
    return EdgeField(
        key=EdgeFieldKey(
            key=edge_key,
            field_language="n/a",
            field_name="academic_year",
        ),
        field_value="2025-2026",
    )


def make_edge_field_list(edge_key: EdgeKey | None = None) -> EdgeFieldList:
    edge_key = edge_key or make_edge_key()
    return EdgeFieldList(
        field_list=[
            make_edge_field_1(edge_key),
            make_edge_field_2(edge_key),
        ]
    )


def make_edge(edge_key: EdgeKey | None = None) -> Edge:
    edge_key = edge_key or make_edge_key()
    return Edge(
        key=edge_key,
        field_list=make_edge_field_list(edge_key),
    )


def test_edge_field_mapper_from_row() -> None:
    edge_key = make_edge_key()
    row = ("en", "role", "Lecturer")

    field = MySQLEdgeFieldMapper.from_row(row, edge_key=edge_key)

    assert isinstance(field, EdgeField)
    assert field.key.key == edge_key
    assert field.key.field_language == "en"
    assert field.key.field_name == "role"
    assert field.field_value == "Lecturer"


def test_edge_field_mapper_from_row_normalizes_none_strings() -> None:
    edge_key = make_edge_key()
    row = (None, None, "value")

    field = MySQLEdgeFieldMapper.from_row(row, edge_key=edge_key)

    assert field.key.field_language == ""
    assert field.key.field_name == ""
    assert field.field_value == "value"


def test_edge_field_mapper_from_dict() -> None:
    edge_key = make_edge_key()
    row = {
        "field_language": "n/a",
        "field_name": "academic_year",
        "field_value": "2025-2026",
    }

    field = MySQLEdgeFieldMapper.from_dict(row, edge_key=edge_key)

    assert isinstance(field, EdgeField)
    assert field.key.key == edge_key
    assert field.key.field_language == "n/a"
    assert field.key.field_name == "academic_year"
    assert field.field_value == "2025-2026"


def test_edge_field_mapper_from_dict_normalizes_missing_values() -> None:
    edge_key = make_edge_key()
    row = {
        "field_language": None,
        "field_name": None,
    }

    field = MySQLEdgeFieldMapper.from_dict(row, edge_key=edge_key)

    assert field.key.field_language == ""
    assert field.key.field_name == ""
    assert field.field_value is None


def test_edge_field_mapper_from_rows() -> None:
    edge_key = make_edge_key()
    rows = [
        ("en", "role", "Lecturer"),
        ("n/a", "academic_year", "2025-2026"),
    ]

    field_list = MySQLEdgeFieldMapper.from_rows(rows, edge_key=edge_key)

    assert isinstance(field_list, EdgeFieldList)
    assert len(field_list.field_list) == 2
    assert field_list.field_list[0].key.key == edge_key
    assert field_list.field_list[0].key.field_name == "role"
    assert field_list.field_list[1].key.field_name == "academic_year"


def test_edge_field_mapper_from_rows_with_none() -> None:
    edge_key = make_edge_key()

    field_list = MySQLEdgeFieldMapper.from_rows(None, edge_key=edge_key)

    assert isinstance(field_list, EdgeFieldList)
    assert field_list.field_list == []


def test_edge_field_mapper_from_dicts() -> None:
    edge_key = make_edge_key()
    rows = [
        {
            "field_language": "en",
            "field_name": "role",
            "field_value": "Lecturer",
        },
        {
            "field_language": "n/a",
            "field_name": "academic_year",
            "field_value": "2025-2026",
        },
    ]

    field_list = MySQLEdgeFieldMapper.from_dicts(rows, edge_key=edge_key)

    assert isinstance(field_list, EdgeFieldList)
    assert len(field_list.field_list) == 2
    assert field_list.field_list[0].key.key == edge_key
    assert field_list.field_list[0].field_value == "Lecturer"
    assert field_list.field_list[1].field_value == "2025-2026"


def test_edge_field_mapper_from_dicts_with_none() -> None:
    edge_key = make_edge_key()

    field_list = MySQLEdgeFieldMapper.from_dicts(None, edge_key=edge_key)

    assert isinstance(field_list, EdgeFieldList)
    assert field_list.field_list == []


def test_edge_field_mapper_to_upsert_row() -> None:
    edge_key = make_edge_key()
    field = make_edge_field_1(edge_key)

    row = MySQLEdgeFieldMapper.to_upsert_row(field)

    assert row == {
        "from_institution_id": "EPFL",
        "from_object_type": "Course",
        "from_object_id": "TEST-101",
        "to_institution_id": "EPFL",
        "to_object_type": "Person",
        "to_object_id": "123456",
        "context": "teacher",
        "field_language": "en",
        "field_name": "role",
        "field_value": "Lecturer",
    }


def test_edge_field_mapper_to_dict() -> None:
    edge_key = make_edge_key()
    field = make_edge_field_2(edge_key)

    row = MySQLEdgeFieldMapper.to_dict(field)

    assert row == {
        "from_institution_id": "EPFL",
        "from_object_type": "Course",
        "from_object_id": "TEST-101",
        "to_institution_id": "EPFL",
        "to_object_type": "Person",
        "to_object_id": "123456",
        "context": "teacher",
        "field_language": "n/a",
        "field_name": "academic_year",
        "field_value": "2025-2026",
    }


def test_edge_field_mapper_to_simplified_row() -> None:
    field = make_edge_field_1()

    row = MySQLEdgeFieldMapper.to_simplified_row(field)

    assert row == {
        "field_language": "en",
        "field_name": "role",
        "field_value": "Lecturer",
    }


def test_edge_field_mapper_to_upsert_rows() -> None:
    field_list = make_edge_field_list()

    rows = MySQLEdgeFieldMapper.to_upsert_rows(field_list)

    assert rows == [
        {
            "from_institution_id": "EPFL",
            "from_object_type": "Course",
            "from_object_id": "TEST-101",
            "to_institution_id": "EPFL",
            "to_object_type": "Person",
            "to_object_id": "123456",
            "context": "teacher",
            "field_language": "en",
            "field_name": "role",
            "field_value": "Lecturer",
        },
        {
            "from_institution_id": "EPFL",
            "from_object_type": "Course",
            "from_object_id": "TEST-101",
            "to_institution_id": "EPFL",
            "to_object_type": "Person",
            "to_object_id": "123456",
            "context": "teacher",
            "field_language": "n/a",
            "field_name": "academic_year",
            "field_value": "2025-2026",
        },
    ]


def test_edge_field_mapper_to_dicts() -> None:
    field_list = make_edge_field_list()

    rows = MySQLEdgeFieldMapper.to_dicts(field_list)

    assert rows == [
        {
            "from_institution_id": "EPFL",
            "from_object_type": "Course",
            "from_object_id": "TEST-101",
            "to_institution_id": "EPFL",
            "to_object_type": "Person",
            "to_object_id": "123456",
            "context": "teacher",
            "field_language": "en",
            "field_name": "role",
            "field_value": "Lecturer",
        },
        {
            "from_institution_id": "EPFL",
            "from_object_type": "Course",
            "from_object_id": "TEST-101",
            "to_institution_id": "EPFL",
            "to_object_type": "Person",
            "to_object_id": "123456",
            "context": "teacher",
            "field_language": "n/a",
            "field_name": "academic_year",
            "field_value": "2025-2026",
        },
    ]


def test_edge_field_mapper_to_simplified_rows() -> None:
    field_list = make_edge_field_list()

    rows = MySQLEdgeFieldMapper.to_simplified_rows(field_list)

    assert rows == [
        {
            "field_language": "en",
            "field_name": "role",
            "field_value": "Lecturer",
        },
        {
            "field_language": "n/a",
            "field_name": "academic_year",
            "field_value": "2025-2026",
        },
    ]


def test_edge_mapper_from_parts_with_rows() -> None:
    edge_key = make_edge_key()
    rows = [
        ("en", "role", "Lecturer"),
        ("n/a", "academic_year", "2025-2026"),
    ]

    edge = MySQLEdgeMapper.from_parts(key=edge_key, custom_field_rows=rows)

    assert isinstance(edge, Edge)
    assert edge.key == edge_key
    assert len(edge.field_list.field_list) == 2
    assert edge.field_list.field_list[0].key.field_name == "role"
    assert edge.field_list.field_list[1].key.field_name == "academic_year"


def test_edge_mapper_from_parts_with_none_rows() -> None:
    edge_key = make_edge_key()

    edge = MySQLEdgeMapper.from_parts(key=edge_key, custom_field_rows=None)

    assert isinstance(edge, Edge)
    assert edge.key == edge_key
    assert edge.field_list.field_list == []


def test_edge_mapper_to_custom_field_rows() -> None:
    edge = make_edge()

    rows = MySQLEdgeMapper.to_custom_field_rows(edge)

    assert rows == [
        {
            "from_institution_id": "EPFL",
            "from_object_type": "Course",
            "from_object_id": "TEST-101",
            "to_institution_id": "EPFL",
            "to_object_type": "Person",
            "to_object_id": "123456",
            "context": "teacher",
            "field_language": "en",
            "field_name": "role",
            "field_value": "Lecturer",
        },
        {
            "from_institution_id": "EPFL",
            "from_object_type": "Course",
            "from_object_id": "TEST-101",
            "to_institution_id": "EPFL",
            "to_object_type": "Person",
            "to_object_id": "123456",
            "context": "teacher",
            "field_language": "n/a",
            "field_name": "academic_year",
            "field_value": "2025-2026",
        },
    ]


def test_edge_mapper_to_simplified_dict() -> None:
    edge = make_edge()

    data = MySQLEdgeMapper.to_simplified_dict(edge)

    assert data == {
        "from_institution_id": "EPFL",
        "from_object_type": "Course",
        "from_object_id": "TEST-101",
        "to_institution_id": "EPFL",
        "to_object_type": "Person",
        "to_object_id": "123456",
        "context": "teacher",
        "custom_fields": [
            {
                "field_language": "en",
                "field_name": "role",
                "field_value": "Lecturer",
            },
            {
                "field_language": "n/a",
                "field_name": "academic_year",
                "field_value": "2025-2026",
            },
        ],
    }


def test_edge_mapper_from_simplified_dict() -> None:
    data = {
        "from_institution_id": "EPFL",
        "from_object_type": "Course",
        "from_object_id": "TEST-101",
        "to_institution_id": "EPFL",
        "to_object_type": "Person",
        "to_object_id": "123456",
        "context": "teacher",
        "custom_fields": [
            {
                "field_language": "en",
                "field_name": "role",
                "field_value": "Lecturer",
            },
            {
                "field_language": "n/a",
                "field_name": "academic_year",
                "field_value": "2025-2026",
            },
        ],
    }

    edge = MySQLEdgeMapper.from_simplified_dict(data)

    assert isinstance(edge, Edge)
    assert edge.key == EdgeKey(
        from_institution_id="EPFL",
        from_object_type="Course",
        from_object_id="TEST-101",
        to_institution_id="EPFL",
        to_object_type="Person",
        to_object_id="123456",
        context="teacher",
    )
    assert len(edge.field_list.field_list) == 2
    assert edge.field_list.field_list[0].key.field_language == "en"
    assert edge.field_list.field_list[0].key.field_name == "role"
    assert edge.field_list.field_list[0].field_value == "Lecturer"
    assert edge.field_list.field_list[1].key.field_language == "n/a"
    assert edge.field_list.field_list[1].key.field_name == "academic_year"
    assert edge.field_list.field_list[1].field_value == "2025-2026"


def test_edge_mapper_from_simplified_dict_without_custom_fields() -> None:
    data = {
        "from_institution_id": "EPFL",
        "from_object_type": "Course",
        "from_object_id": "TEST-101",
        "to_institution_id": "EPFL",
        "to_object_type": "Person",
        "to_object_id": "123456",
        "context": "teacher",
    }

    edge = MySQLEdgeMapper.from_simplified_dict(data)

    assert isinstance(edge, Edge)
    assert edge.key == EdgeKey(
        from_institution_id="EPFL",
        from_object_type="Course",
        from_object_id="TEST-101",
        to_institution_id="EPFL",
        to_object_type="Person",
        to_object_id="123456",
        context="teacher",
    )
    assert edge.field_list.field_list == []


def test_edge_mapper_to_simplified_dict_list() -> None:
    edge_1 = make_edge()
    edge_2_key = EdgeKey(
        from_institution_id="EPFL",
        from_object_type="Course",
        from_object_id="TEST-102",
        to_institution_id="EPFL",
        to_object_type="Person",
        to_object_id="654321",
        context="assistant",
    )
    edge_2 = Edge(
        key=edge_2_key,
        field_list=EdgeFieldList(
            field_list=[
                EdgeField(
                    key=EdgeFieldKey(
                        key=edge_2_key,
                        field_language="en",
                        field_name="role",
                    ),
                    field_value="Teaching Assistant",
                )
            ]
        ),
    )

    data = MySQLEdgeMapper.to_simplified_dict_list([edge_1, edge_2])

    assert data == [
        {
            "from_institution_id": "EPFL",
            "from_object_type": "Course",
            "from_object_id": "TEST-101",
            "to_institution_id": "EPFL",
            "to_object_type": "Person",
            "to_object_id": "123456",
            "context": "teacher",
            "custom_fields": [
                {
                    "field_language": "en",
                    "field_name": "role",
                    "field_value": "Lecturer",
                },
                {
                    "field_language": "n/a",
                    "field_name": "academic_year",
                    "field_value": "2025-2026",
                },
            ],
        },
        {
            "from_institution_id": "EPFL",
            "from_object_type": "Course",
            "from_object_id": "TEST-102",
            "to_institution_id": "EPFL",
            "to_object_type": "Person",
            "to_object_id": "654321",
            "context": "assistant",
            "custom_fields": [
                {
                    "field_language": "en",
                    "field_name": "role",
                    "field_value": "Teaching Assistant",
                }
            ],
        },
    ]


def test_edge_mapper_from_simplified_dict_list() -> None:
    data = [
        {
            "from_institution_id": "EPFL",
            "from_object_type": "Course",
            "from_object_id": "TEST-101",
            "to_institution_id": "EPFL",
            "to_object_type": "Person",
            "to_object_id": "123456",
            "context": "teacher",
            "custom_fields": [
                {
                    "field_language": "en",
                    "field_name": "role",
                    "field_value": "Lecturer",
                }
            ],
        },
        {
            "from_institution_id": "EPFL",
            "from_object_type": "Course",
            "from_object_id": "TEST-102",
            "to_institution_id": "EPFL",
            "to_object_type": "Person",
            "to_object_id": "654321",
            "context": "assistant",
            "custom_fields": [
                {
                    "field_language": "n/a",
                    "field_name": "academic_year",
                    "field_value": "2025-2026",
                }
            ],
        },
    ]

    edges = MySQLEdgeMapper.from_simplified_dict_list(data)

    assert len(edges) == 2

    assert edges[0].key == EdgeKey(
        from_institution_id="EPFL",
        from_object_type="Course",
        from_object_id="TEST-101",
        to_institution_id="EPFL",
        to_object_type="Person",
        to_object_id="123456",
        context="teacher",
    )
    assert len(edges[0].field_list.field_list) == 1
    assert edges[0].field_list.field_list[0].field_value == "Lecturer"

    assert edges[1].key == EdgeKey(
        from_institution_id="EPFL",
        from_object_type="Course",
        from_object_id="TEST-102",
        to_institution_id="EPFL",
        to_object_type="Person",
        to_object_id="654321",
        context="assistant",
    )
    assert len(edges[1].field_list.field_list) == 1
    assert edges[1].field_list.field_list[0].field_value == "2025-2026"


def test_edge_mapper_from_simplified_dict_list_with_none() -> None:
    edges = MySQLEdgeMapper.from_simplified_dict_list(None)  # type: ignore[arg-type]
    assert edges == []


def test_edge_mapper_round_trip_simplified_dict() -> None:
    edge = make_edge()

    data = MySQLEdgeMapper.to_simplified_dict(edge)
    rebuilt = MySQLEdgeMapper.from_simplified_dict(data)

    assert rebuilt.key == edge.key
    assert len(rebuilt.field_list.field_list) == len(edge.field_list.field_list)

    rebuilt_rows = MySQLEdgeMapper.to_simplified_dict(rebuilt)
    assert rebuilt_rows == data


def test_edge_mapper_round_trip_simplified_dict_list() -> None:
    edges = [make_edge(), make_edge()]

    data = MySQLEdgeMapper.to_simplified_dict_list(edges)
    rebuilt = MySQLEdgeMapper.from_simplified_dict_list(data)

    assert len(rebuilt) == 2
    assert MySQLEdgeMapper.to_simplified_dict_list(rebuilt) == data
