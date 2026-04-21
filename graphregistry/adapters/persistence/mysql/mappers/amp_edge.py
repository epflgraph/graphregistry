# graphregistry/adapters/persistence/mysql/mappers/amp_edge.py
from __future__ import annotations
from typing import Any
from graphregistry.domain.models.entities.mdl_base import EdgeFieldKey, EdgeKey
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeField, EdgeFieldList

# Class definition
class MySQLEdgeFieldMapper:
    """
    Maps between MySQL custom-field row shapes and domain EdgeField / EdgeFieldList.
    """

    @staticmethod
    def from_row(row: tuple[Any, ...], edge_key: EdgeKey) -> EdgeField:
        """
        Expected row shape:
            (field_language, field_name, field_value)
        """
        field_language, field_name, field_value = row
        return EdgeField(
            key=EdgeFieldKey(
                key=edge_key,
                field_language=str(field_language or ""),
                field_name=str(field_name or ""),
            ),
            field_value=field_value,
        )

    @staticmethod
    def from_dict(row: dict[str, Any], edge_key: EdgeKey) -> EdgeField:
        """
        Expected dict shape:
            {
                "field_language": ...,
                "field_name": ...,
                "field_value": ...
            }
        """
        return EdgeField(
            key=EdgeFieldKey(
                key=edge_key,
                field_language=str(row.get("field_language") or ""),
                field_name=str(row.get("field_name") or ""),
            ),
            field_value=row.get("field_value"),
        )

    @staticmethod
    def from_rows(rows: list[tuple[Any, ...]] | None, edge_key: EdgeKey) -> EdgeFieldList:
        """
        Expected row shape:
            (field_language, field_name, field_value)
        """
        return EdgeFieldList(
            field_list=[
                MySQLEdgeFieldMapper.from_row(row, edge_key=edge_key)
                for row in (rows or [])
            ]
        )

    @staticmethod
    def from_dicts(rows: list[dict[str, Any]] | None, edge_key: EdgeKey) -> EdgeFieldList:
        """
        Expected dict shape:
            {
                "field_language": ...,
                "field_name": ...,
                "field_value": ...
            }
        """
        return EdgeFieldList(
            field_list=[
                MySQLEdgeFieldMapper.from_dict(row, edge_key=edge_key)
                for row in (rows or [])
            ]
        )

    @staticmethod
    def to_upsert_row(field: EdgeField) -> dict[str, Any]:
        """
        Returns one row suitable for upserting into Data_N_Object_N_Object_T_CustomFields.
        """
        return {
            "from_institution_id": field.key.key.from_institution_id,
            "from_object_type": field.key.key.from_object_type,
            "from_object_id": field.key.key.from_object_id,
            "to_institution_id": field.key.key.to_institution_id,
            "to_object_type": field.key.key.to_object_type,
            "to_object_id": field.key.key.to_object_id,
            "context": field.key.key.context,
            "field_language": field.key.field_language,
            "field_name": field.key.field_name,
            "field_value": field.field_value,
        }

    @staticmethod
    def to_dict(field: EdgeField) -> dict[str, Any]:
        """
        Returns a full dict representation including parent edge identity fields.
        """
        return {
            "from_institution_id": field.key.key.from_institution_id,
            "from_object_type": field.key.key.from_object_type,
            "from_object_id": field.key.key.from_object_id,
            "to_institution_id": field.key.key.to_institution_id,
            "to_object_type": field.key.key.to_object_type,
            "to_object_id": field.key.key.to_object_id,
            "context": field.key.key.context,
            "field_language": field.key.field_language,
            "field_name": field.key.field_name,
            "field_value": field.field_value,
        }

    @staticmethod
    def to_simplified_row(field: EdgeField) -> dict[str, Any]:
        """
        Returns simplified row without parent edge identity fields.
        Useful for export/debug payloads.
        """
        return {
            "field_language": field.key.field_language,
            "field_name": field.key.field_name,
            "field_value": field.field_value,
        }

    @staticmethod
    def to_upsert_rows(field_list: EdgeFieldList) -> list[dict[str, Any]]:
        """
        Returns rows suitable for upserting into Data_N_Object_N_Object_T_CustomFields.
        """
        return [
            MySQLEdgeFieldMapper.to_upsert_row(field)
            for field in field_list.field_list
        ]

    @staticmethod
    def to_dicts(field_list: EdgeFieldList) -> list[dict[str, Any]]:
        """
        Returns full dict rows including parent edge identity fields.
        """
        return [
            MySQLEdgeFieldMapper.to_dict(field)
            for field in field_list.field_list
        ]

    @staticmethod
    def to_simplified_rows(field_list: EdgeFieldList) -> list[dict[str, Any]]:
        """
        Returns simplified rows without the parent edge identity fields.
        Useful for export/debug payloads.
        """
        return [
            MySQLEdgeFieldMapper.to_simplified_row(field)
            for field in field_list.field_list
        ]

# Class definition
class MySQLEdgeMapper:
    """
    Maps between MySQL row shapes and the domain Edge model.
    """

    @staticmethod
    def from_parts(
        key: EdgeKey,
        custom_field_rows: list[tuple[Any, ...]] | None = None,
    ) -> Edge:
        return Edge(
            key=key,
            field_list=MySQLEdgeFieldMapper.from_rows(custom_field_rows, edge_key=key),
        )

    @staticmethod
    def to_custom_field_rows(edge: Edge) -> list[dict[str, Any]]:
        return MySQLEdgeFieldMapper.to_upsert_rows(edge.field_list)

    @staticmethod
    def to_simplified_dict(edge: Edge) -> dict[str, Any]:
        return {
            "from_institution_id": edge.key.from_institution_id,
            "from_object_type": edge.key.from_object_type,
            "from_object_id": edge.key.from_object_id,
            "to_institution_id": edge.key.to_institution_id,
            "to_object_type": edge.key.to_object_type,
            "to_object_id": edge.key.to_object_id,
            "context": edge.key.context,
            "custom_fields": MySQLEdgeFieldMapper.to_simplified_rows(edge.field_list),
        }

    @staticmethod
    def from_simplified_dict(data: dict[str, Any]) -> Edge:
        key = EdgeKey(
            from_institution_id=str(data["from_institution_id"]),
            from_object_type=str(data["from_object_type"]),
            from_object_id=str(data["from_object_id"]),
            to_institution_id=str(data["to_institution_id"]),
            to_object_type=str(data["to_object_type"]),
            to_object_id=str(data["to_object_id"]),
            context=str(data["context"]),
        )

        return Edge(
            key=key,
            field_list=MySQLEdgeFieldMapper.from_dicts(data.get("custom_fields"), edge_key=key),
        )

    @staticmethod
    def to_simplified_dict_list(edge_list: list[Edge]) -> list[dict[str, Any]]:
        return [MySQLEdgeMapper.to_simplified_dict(edge) for edge in edge_list]

    @staticmethod
    def from_simplified_dict_list(data: list[dict[str, Any]]) -> list[Edge]:
        return [MySQLEdgeMapper.from_simplified_dict(item) for item in (data or [])]
