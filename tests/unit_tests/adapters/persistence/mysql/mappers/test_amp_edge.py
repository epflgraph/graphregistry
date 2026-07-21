# tests/unit_tests/adapters/persistence/mysql/mappers/test_amp_edge.py
"""Unit tests for the MySQL edge mapper using synthetic row data."""
from __future__ import annotations

from graphregistry.adapters.persistence.mysql.mappers.amp_edge import MySQLEdgeFieldMapper, MySQLEdgeMapper
from graphregistry.domain.models.entities.mdl_base import EdgeFieldKey, EdgeKey
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeField, EdgeFieldList, EdgeList


class TestMySQLEdgeFieldMapper:
    def test_from_row(self) -> None:
        key = EdgeKey(
            from_institution_id="EPFL", from_object_type="Course", from_object_id="CS-433",
            to_institution_id="EPFL", to_object_type="Person", to_object_id="p-1",
            context="taught_by",
        )
        field = MySQLEdgeFieldMapper.from_row(("en", "semester", "fall"), edge_key=key)
        assert field.key == EdgeFieldKey(key=key, field_language="en", field_name="semester")
        assert field.field_value == "fall"

    def test_to_upsert_row(self) -> None:
        key = EdgeKey(
            from_institution_id="EPFL", from_object_type="Course", from_object_id="CS-433",
            to_institution_id="EPFL", to_object_type="Person", to_object_id="p-1",
            context="taught_by",
        )
        field = EdgeField(key=EdgeFieldKey(key=key, field_language="en", field_name="semester"), field_value="fall")
        row = MySQLEdgeFieldMapper.to_upsert_row(field)
        assert row["context"] == "taught_by"
        assert row["field_name"] == "semester"
        assert row["field_value"] == "fall"
        assert row["record_deleted"] == 0


class TestMySQLEdgeMapper:
    def test_from_parts(self) -> None:
        key = EdgeKey(
            from_institution_id="EPFL", from_object_type="Course", from_object_id="CS-433",
            to_institution_id="EPFL", to_object_type="Person", to_object_id="p-1",
            context="taught_by",
        )
        edge = MySQLEdgeMapper.from_parts(key=key, custom_field_rows=[("en", "semester", "fall")])
        assert edge.key == key
        assert len(edge.field_list.item_list) == 1

    def test_to_basic_row_sets_record_deleted(self) -> None:
        key = EdgeKey(
            from_institution_id="EPFL", from_object_type="Course", from_object_id="CS-433",
            to_institution_id="EPFL", to_object_type="Person", to_object_id="p-1",
            context="taught_by",
        )
        edge = Edge(key=key)
        assert MySQLEdgeMapper.to_basic_row(edge) == {"record_deleted": 0}

    def test_to_simplified_dict_roundtrip(self) -> None:
        key = EdgeKey(
            from_institution_id="EPFL", from_object_type="Course", from_object_id="CS-433",
            to_institution_id="EPFL", to_object_type="Person", to_object_id="p-1",
            context="taught_by",
        )
        field = EdgeField(key=EdgeFieldKey(key=key, field_language="en", field_name="semester"), field_value="fall")
        edge = Edge(key=key, field_list=EdgeFieldList(item_list=[field]))

        data = MySQLEdgeMapper.to_simplified_dict(edge)
        rebuilt = MySQLEdgeMapper.from_simplified_dict(data)
        assert rebuilt.key == key
        assert rebuilt.field_list.item_list[0].field_value == "fall"

    def test_simplified_dict_list_roundtrip(self) -> None:
        key = EdgeKey(
            from_institution_id="EPFL", from_object_type="Course", from_object_id="CS-433",
            to_institution_id="EPFL", to_object_type="Person", to_object_id="p-1",
            context="taught_by",
        )
        edge = Edge(key=key)
        data_list = MySQLEdgeMapper.to_simplified_dict_list(EdgeList(item_list=[edge]))
        rebuilt = MySQLEdgeMapper.from_simplified_dict_list(data_list)
        assert len(rebuilt.item_list) == 1
