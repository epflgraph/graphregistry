# graphregistry/adapters/persistence/mysql/mappers/amp_node.py
from __future__ import annotations
from typing import Any
from graphregistry.adapters.persistence.mysql.mappers.amp_pageprofile import MySQLPageProfileMapper
from graphregistry.domain.models.mdl_base import NodeFieldKey, NodeKey
from graphregistry.domain.models.mdl_node import Node, NodeField, NodeFieldList

# Class definition
class MySQLNodeFieldMapper:
    """
    Maps between MySQL custom-field row shapes and domain NodeField / NodeFieldList.
    """

    @staticmethod
    def from_rows(rows: list[tuple[Any, ...]] | None, node_key: NodeKey) -> NodeFieldList:
        """
        Expected row shape:
            (field_language, field_name, field_value)
        """
        field_list: list[NodeField] = []

        for row in rows or []:
            field_language, field_name, field_value = row
            field_list.append(
                NodeField(
                    key=NodeFieldKey(
                        key=node_key,
                        field_language=str(field_language or ""),
                        field_name=str(field_name or ""),
                    ),
                    field_value=field_value,
                )
            )

        return NodeFieldList(field_list=field_list)

    @staticmethod
    def from_dicts(rows: list[dict[str, Any]] | None, node_key: NodeKey) -> NodeFieldList:
        """
        Expected dict shape:
            {
                "field_language": ...,
                "field_name": ...,
                "field_value": ...
            }
        """
        field_list: list[NodeField] = []

        for row in rows or []:
            field_list.append(
                NodeField(
                    key=NodeFieldKey(
                        key=node_key,
                        field_language=str(row.get("field_language") or ""),
                        field_name=str(row.get("field_name") or ""),
                    ),
                    field_value=row.get("field_value"),
                )
            )

        return NodeFieldList(field_list=field_list)

    @staticmethod
    def to_upsert_rows(field_list: NodeFieldList) -> list[dict[str, Any]]:
        """
        Returns rows suitable for upserting into Data_N_Object_T_CustomFields.
        """
        rows: list[dict[str, Any]] = []

        for field in field_list.field_list:
            rows.append(
                {
                    "institution_id": field.key.key.institution_id,
                    "object_type": field.key.key.object_type,
                    "object_id": field.key.key.object_id,
                    "field_language": field.key.field_language,
                    "field_name": field.key.field_name,
                    "field_value": field.field_value,
                }
            )

        return rows

    @staticmethod
    def to_simplified_rows(field_list: NodeFieldList) -> list[dict[str, Any]]:
        """
        Returns simplified rows without the parent node identity fields.
        Useful for export/debug payloads.
        """
        rows: list[dict[str, Any]] = []

        for field in field_list.field_list:
            rows.append(
                {
                    "field_language": field.key.field_language,
                    "field_name": field.key.field_name,
                    "field_value": field.field_value,
                }
            )

        return rows


class MySQLNodeMapper:
    """
    Maps between MySQL row shapes and the domain Node model.
    """

    @staticmethod
    def from_parts(
        key: NodeKey,
        basic_row: tuple[Any, ...] | None,
        custom_field_rows: list[tuple[Any, ...]] | None = None,
        page_profile_row: dict[str, Any] | None = None,
    ) -> Node:
        """
        Build a Node from the separate SQL result parts.

        Expected basic_row shape:
            (object_title, text_source, raw_text)
        """
        title = ""
        text_source = ""
        raw_text = ""

        if basic_row is not None:
            title, text_source, raw_text = basic_row

        return Node(
            key=key,
            title=str(title or ""),
            text_source=str(text_source or ""),
            raw_text=str(raw_text or ""),
            field_list=MySQLNodeFieldMapper.from_rows(custom_field_rows, node_key=key),
            page_profile=MySQLPageProfileMapper.from_row(page_profile_row, key=key),
        )

    @staticmethod
    def to_basic_row(node: Node) -> dict[str, Any]:
        """
        Returns payload suitable for upserting into Nodes_N_Object.
        Identity columns are omitted.
        """
        return {
            "object_title": node.title,
            "text_source": node.text_source,
            "raw_text": node.raw_text,
        }

    @staticmethod
    def to_custom_field_rows(node: Node) -> list[dict[str, Any]]:
        """
        Returns rows suitable for upserting into Data_N_Object_T_CustomFields.
        """
        return MySQLNodeFieldMapper.to_upsert_rows(node.field_list)

    @staticmethod
    def to_page_profile_row(node: Node) -> dict[str, Any]:
        """
        Returns payload suitable for upserting into Data_N_Object_T_PageProfile.
        Identity columns are omitted.
        """
        assert node.page_profile is not None
        return MySQLPageProfileMapper.to_row(node.page_profile)

    @staticmethod
    def to_simplified_dict(node: Node) -> dict[str, Any]:
        """
        Returns a portable simplified representation of a Node.
        Useful for export/debug payloads.
        """
        return {
            "institution_id": node.key.institution_id,
            "object_type": node.key.object_type,
            "object_id": node.key.object_id,
            "object_title": node.title,
            "text_source": node.text_source,
            "raw_text": node.raw_text,
            "custom_fields": MySQLNodeFieldMapper.to_simplified_rows(node.field_list),
            "page_profile": MySQLNodeMapper.to_page_profile_row(node),
        }

    @staticmethod
    def from_simplified_dict(data: dict[str, Any]) -> Node:
        key = NodeKey(
            institution_id=str(data["institution_id"]),
            object_type=str(data["object_type"]),
            object_id=str(data["object_id"]),
        )

        return Node(
            key=key,
            title=str(data.get("object_title") or ""),
            text_source=str(data.get("text_source") or ""),
            raw_text=str(data.get("raw_text") or ""),
            field_list=MySQLNodeFieldMapper.from_dicts(data.get("custom_fields"), node_key=key),
            page_profile=MySQLPageProfileMapper.from_row(data.get("page_profile"), key=key),
        )

    @staticmethod
    def to_simplified_dict_list(node_list: list[Node]) -> list[dict[str, Any]]:
        return [MySQLNodeMapper.to_simplified_dict(node) for node in node_list]

    @staticmethod
    def from_simplified_dict_list(data: list[dict[str, Any]]) -> list[Node]:
        return [MySQLNodeMapper.from_simplified_dict(item) for item in (data or [])]