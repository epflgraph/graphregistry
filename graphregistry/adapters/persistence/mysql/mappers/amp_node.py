# graphregistry/adapters/persistence/mysql/mappers/amp_node.py
from __future__ import annotations
from typing import Any
from graphregistry.adapters.persistence.mysql.mappers.amp_pageprofile import MySQLPageProfileMapper
from graphregistry.domain.models.entities.mdl_base import NodeFieldKey, NodeKey
from graphregistry.domain.models.entities.mdl_node import Node, NodeField, NodeFieldList
from graphregistry.domain.models.tasks.mdl_conceptdet import ConceptDetectionResult, ConceptDetectionResultList

# Class definition
class MySQLNodeFieldMapper:
    """
    Maps between MySQL custom-field row shapes and domain NodeField / NodeFieldList.
    """

    @staticmethod
    def from_row(row: tuple[Any, ...], node_key: NodeKey) -> NodeField:
        """
        Expected row shape:
            (field_language, field_name, field_value)
        """
        field_language, field_name, field_value = row
        return NodeField(
            key=NodeFieldKey(
                key=node_key,
                field_language=str(field_language or ""),
                field_name=str(field_name or ""),
            ),
            field_value=field_value,
        )

    @staticmethod
    def from_dict(row: dict[str, Any], node_key: NodeKey) -> NodeField:
        """
        Expected dict shape:
            {
                "field_language": ...,
                "field_name": ...,
                "field_value": ...
            }
        """
        return NodeField(
            key=NodeFieldKey(
                key=node_key,
                field_language=str(row.get("field_language") or ""),
                field_name=str(row.get("field_name") or ""),
            ),
            field_value=row.get("field_value"),
        )

    @staticmethod
    def from_rows(rows: list[tuple[Any, ...]] | None, node_key: NodeKey) -> NodeFieldList:
        """
        Expected row shape:
            (field_language, field_name, field_value)
        """
        return NodeFieldList(
            item_list=[
                MySQLNodeFieldMapper.from_row(row, node_key=node_key)
                for row in (rows or [])
            ]
        )

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
        return NodeFieldList(
            item_list=[
                MySQLNodeFieldMapper.from_dict(row, node_key=node_key)
                for row in (rows or [])
            ]
        )

    @staticmethod
    def to_custom_field_upsert_row(field: NodeField) -> dict[str, Any]:
        """
        Returns one row suitable for upserting into Data_N_Object_T_CustomFields.
        """
        return {
            "institution_id" : field.key.key.institution_id,
            "object_type"    : field.key.key.object_type,
            "object_id"      : field.key.key.object_id,
            "field_language" : field.key.field_language,
            "field_name"     : field.key.field_name,
            "field_value"    : field.field_value,
        }

    @staticmethod
    def to_detected_concepts_upsert_row(node_key: NodeKey, text_source: str, concept: ConceptDetectionResult) -> dict[str, Any]:
        """
        Returns one row suitable for upserting into Edges_N_Object_N_Concept_T_ConceptDetection.
        """
        return {
            "institution_id" : node_key.institution_id,
            "object_type"    : node_key.object_type,
            "object_id"      : node_key.object_id,
            "concept_id"     : concept.concept_id,
            "text_source"    : text_source,
            "score"          : concept.score
        }

    @staticmethod
    def to_dict(field: NodeField) -> dict[str, Any]:
        """
        Returns a full dict representation including parent node identity fields.
        """
        return {
            "institution_id": field.key.key.institution_id,
            "object_type": field.key.key.object_type,
            "object_id": field.key.key.object_id,
            "field_language": field.key.field_language,
            "field_name": field.key.field_name,
            "field_value": field.field_value,
        }

    @staticmethod
    def to_simplified_row(field: NodeField) -> dict[str, Any]:
        """
        Returns simplified row without parent node identity fields.
        Useful for export/debug payloads.
        """
        return {
            "field_language": field.key.field_language,
            "field_name": field.key.field_name,
            "field_value": field.field_value,
        }

    @staticmethod
    def to_custom_field_upsert_rows(field_list: NodeFieldList) -> list[dict[str, Any]]:
        """
        Returns rows suitable for upserting into Data_N_Object_T_CustomFields.
        """
        return [
            MySQLNodeFieldMapper.to_custom_field_upsert_row(field)
            for field in field_list.item_list
        ]

    @staticmethod
    def to_detected_concepts_upsert_rows(node_key: NodeKey, text_source: str, detected_concepts: ConceptDetectionResultList) -> list[dict[str, Any]]:
        """
        Returns rows suitable for upserting into Edges_N_Object_N_Concept_T_ConceptDetection.
        """
        return [
            MySQLNodeFieldMapper.to_detected_concepts_upsert_row(node_key, text_source, concept)
            for concept in detected_concepts.item_list
        ]

    @staticmethod
    def to_dicts(field_list: NodeFieldList) -> list[dict[str, Any]]:
        """
        Returns full dict rows including parent node identity fields.
        """
        return [
            MySQLNodeFieldMapper.to_dict(field)
            for field in field_list.item_list
        ]

    @staticmethod
    def to_simplified_rows(field_list: NodeFieldList) -> list[dict[str, Any]]:
        """
        Returns simplified rows without the parent node identity fields.
        Useful for export/debug payloads.
        """
        return [
            MySQLNodeFieldMapper.to_simplified_row(field)
            for field in field_list.item_list
        ]

# Class definition
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
        return MySQLNodeFieldMapper.to_custom_field_upsert_rows(node.field_list)

    @staticmethod
    def to_page_profile_row(node: Node) -> dict[str, Any]:
        """
        Returns payload suitable for upserting into Data_N_Object_T_PageProfile.
        Identity columns are omitted.
        """
        assert node.page_profile is not None
        return MySQLPageProfileMapper.to_row(node.page_profile)

    @staticmethod
    def to_detected_concepts_rows(node: Node) -> list[dict[str, Any]]:
        """
        Returns rows suitable for upserting into Data_N_Object_T_CustomFields.
        """
        return MySQLNodeFieldMapper.to_detected_concepts_upsert_rows(node.key, node.text_source, node.detected_concepts)

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
