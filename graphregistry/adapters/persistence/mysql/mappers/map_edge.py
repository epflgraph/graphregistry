# graphregistry/adapters/persistence/mysql/mappers/amp_edge.py
from __future__ import annotations
from typing import Any
from graphregistry.domain.models.entities.mdl_base import EdgeFieldKey, EdgeKey
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeList, EdgeField, EdgeFieldList

# Class definition
class MySQLEdgeFieldMapper:
    """
    Maps between MySQL custom-field row shapes and domain EdgeField / EdgeFieldList.
    """

    @staticmethod
    def from_row(row: tuple[Any, ...], edge_key: EdgeKey) -> EdgeField:
        field_language, field_name, field_value = row
        return EdgeField(
            key=EdgeFieldKey(
                key            = edge_key,
                field_language = field_language,
                field_name     = field_name,
            ),
            field_value = field_value,
        )

    @staticmethod
    def from_dict(row: dict[str, Any], edge_key: EdgeKey) -> EdgeField:
        return EdgeField(
            key=EdgeFieldKey(
                key            = edge_key,
                field_language = row['field_language'],
                field_name     = row['field_name'],
            ),
            field_value = row['field_value'],
        )

    @staticmethod
    def from_rows(rows: list[tuple[Any, ...]] | None, edge_key: EdgeKey) -> EdgeFieldList:
        return EdgeFieldList(
            item_list=[
                MySQLEdgeFieldMapper.from_row(row, edge_key=edge_key)
                for row in (rows or [])
            ]
        )

    @staticmethod
    def from_dict_list(rows: list[dict[str, Any]] | None, edge_key: EdgeKey) -> EdgeFieldList:
        return EdgeFieldList(
            item_list=[
                MySQLEdgeFieldMapper.from_dict(row, edge_key=edge_key)
                for row in (rows or [])
            ]
        )

    @staticmethod
    def to_upsert_row(field: EdgeField) -> dict[str, Any]:
        return {
            'from_object_type'    : field.key.key.from_object_type,
            'from_object_id'      : field.key.key.from_object_id,
            'to_object_type'      : field.key.key.to_object_type,
            'to_object_id'        : field.key.key.to_object_id,
            'context'             : field.key.key.context,
            'field_language'      : field.key.field_language,
            'field_name'          : field.key.field_name,
            'field_value'         : field.field_value,
            'record_deleted'      : 0,
        }

    @staticmethod
    def to_dict(field: EdgeField) -> dict[str, Any]:
        return {
            'from_object_type'    : field.key.key.from_object_type,
            'from_object_id'      : field.key.key.from_object_id,
            'to_object_type'      : field.key.key.to_object_type,
            'to_object_id'        : field.key.key.to_object_id,
            'context'             : field.key.key.context,
            'field_language'      : field.key.field_language,
            'field_name'          : field.key.field_name,
            'field_value'         : field.field_value,
        }

    @staticmethod
    def to_simplified_row(field: EdgeField) -> dict[str, Any]:
        return {
            'field_language' : field.key.field_language,
            'field_name'     : field.key.field_name,
            'field_value'    : field.field_value,
        }

    @staticmethod
    def to_upsert_rows(field_list: EdgeFieldList) -> list[dict[str, Any]]:
        return [MySQLEdgeFieldMapper.to_upsert_row(field) for field in field_list.item_list]

    @staticmethod
    def to_dict_list(field_list: EdgeFieldList) -> list[dict[str, Any]]:
        return [MySQLEdgeFieldMapper.to_dict(field) for field in field_list.item_list]

    @staticmethod
    def to_simplified_rows(field_list: EdgeFieldList) -> list[dict[str, Any]]:
        return [MySQLEdgeFieldMapper.to_simplified_row(field) for field in field_list.item_list]

# Class definition
class MySQLEdgeMapper:
    """
    Maps between MySQL row shapes and the domain Edge model.
    """

    @staticmethod
    def from_parts(key: EdgeKey, custom_field_rows: list[tuple[Any, ...]] | None = None) -> Edge:
        """
        Build an Edge from the separate SQL result parts.
        """
        return Edge(key=key, field_list=MySQLEdgeFieldMapper.from_rows(custom_field_rows, edge_key=key))

    @staticmethod
    def to_basic_row(edge: Edge) -> dict[str, Any]:
        """
        Return the persistence row for the edge shell.

        The edge key columns are handled by the repository upsert; this row
        contains only the non-key fields that must be reset on save, including
        record_deleted so that re-saving undeletes a soft-deleted edge.
        """
        return {'record_deleted': 0}

    @staticmethod
    def to_custom_field_rows(edge: Edge) -> list[dict[str, Any]]:
        return MySQLEdgeFieldMapper.to_upsert_rows(edge.field_list)

    @staticmethod
    def to_simplified_dict(edge: Edge) -> dict[str, Any]:
        return {
            'from_object_type'    : edge.key.from_object_type,
            'from_object_id'      : edge.key.from_object_id,
            'to_object_type'      : edge.key.to_object_type,
            'to_object_id'        : edge.key.to_object_id,
            'context'             : edge.key.context,
            'custom_fields'       : MySQLEdgeFieldMapper.to_simplified_rows(edge.field_list),
        }

    @staticmethod
    def from_simplified_dict(data: dict[str, Any]) -> Edge:
        key = EdgeKey(
            from_object_type    = data['from_object_type'],
            from_object_id      = data['from_object_id'],
            to_object_type      = data['to_object_type'],
            to_object_id        = data['to_object_id'],
            context             = data['context'],
        )
        return Edge(key=key, field_list=MySQLEdgeFieldMapper.from_dict_list(data['custom_fields'], edge_key=key))

    @staticmethod
    def to_simplified_dict_list(edge_list: EdgeList | list[Edge]) -> list[dict[str, Any]]:
        if isinstance(edge_list, EdgeList):
            edge_list = edge_list.item_list
        return [MySQLEdgeMapper.to_simplified_dict(edge) for edge in edge_list]

    @staticmethod
    def from_simplified_dict_list(data: list[dict[str, Any]]) -> EdgeList:
        return EdgeList(item_list=[MySQLEdgeMapper.from_simplified_dict(item) for item in (data or [])])
