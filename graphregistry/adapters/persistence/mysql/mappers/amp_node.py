# graphregistry/adapters/persistence/mysql/mappers/amp_node.py
from __future__ import annotations
from typing import Any
from graphregistry.adapters.persistence.mysql.mappers.amp_conceptdet import MySQLConceptDetectionResultMapper
from graphregistry.adapters.persistence.mysql.mappers.amp_pageprofile import MySQLPageProfileMapper
from graphregistry.domain.models.entities.mdl_base import NodeFieldKey, NodeKey
from graphregistry.domain.models.entities.mdl_node import Node, NodeList, NodeField, NodeFieldList

# Class definition
class MySQLNodeFieldMapper:
    """
    Maps between MySQL custom-field row shapes and domain NodeField / NodeFieldList.
    """

    @staticmethod
    def from_row(row: tuple[Any, ...], node_key: NodeKey) -> NodeField:
        field_language, field_name, field_value = row
        return NodeField(
            key=NodeFieldKey(
                key            = node_key,
                field_language = field_language,
                field_name     = field_name,
            ),
            field_value = field_value,
        )

    @staticmethod
    def from_dict(row: dict[str, Any], node_key: NodeKey) -> NodeField:
        return NodeField(
            key=NodeFieldKey(
                key            = node_key,
                field_language = row['field_language'],
                field_name     = row['field_name'],
            ),
            field_value = row['field_value'],
        )

    @staticmethod
    def from_rows(rows: list[tuple[Any, ...]] | None, node_key: NodeKey) -> NodeFieldList:
        return NodeFieldList(
            item_list=[
                MySQLNodeFieldMapper.from_row(row, node_key=node_key)
                for row in (rows or [])
            ]
        )

    @staticmethod
    def from_dict_list(rows: list[dict[str, Any]] | None, node_key: NodeKey) -> NodeFieldList:
        return NodeFieldList(
            item_list=[
                MySQLNodeFieldMapper.from_dict(row, node_key=node_key)
                for row in (rows or [])
            ]
        )

    @staticmethod
    def to_upsert_row(field: NodeField) -> dict[str, Any]:
        return {
            'institution_id' : field.key.key.institution_id,
            'object_type'    : field.key.key.object_type,
            'object_id'      : field.key.key.object_id,
            'field_language' : field.key.field_language,
            'field_name'     : field.key.field_name,
            'field_value'    : field.field_value,
        }

    @staticmethod
    def to_dict(field: NodeField) -> dict[str, Any]:
        return {
            'institution_id' : field.key.key.institution_id,
            'object_type'    : field.key.key.object_type,
            'object_id'      : field.key.key.object_id,
            'field_language' : field.key.field_language,
            'field_name'     : field.key.field_name,
            'field_value'    : field.field_value,
        }

    @staticmethod
    def to_simplified_row(field: NodeField) -> dict[str, Any]:
        return {
            'field_language' : field.key.field_language,
            'field_name'     : field.key.field_name,
            'field_value'    : field.field_value,
        }

    @staticmethod
    def to_upsert_rows(field_list: NodeFieldList) -> list[dict[str, Any]]:
        return [MySQLNodeFieldMapper.to_upsert_row(field) for field in field_list.item_list]

    @staticmethod
    def to_dict_list(field_list: NodeFieldList) -> list[dict[str, Any]]:
        return [MySQLNodeFieldMapper.to_dict(field) for field in field_list.item_list]

    @staticmethod
    def to_simplified_rows(field_list: NodeFieldList) -> list[dict[str, Any]]:
        return [MySQLNodeFieldMapper.to_simplified_row(field) for field in field_list.item_list]

# Class definition
class MySQLNodeMapper:
    """
    Maps between MySQL row shapes and the domain Node model.
    """

    @staticmethod
    def from_parts(
        key               : NodeKey,
        basic_row         : tuple[Any, ...] | None,
        custom_field_rows : list[tuple[Any, ...]] | None = None,
        page_profile_row  : dict[str, Any] | None = None,
        concept_rows      : list[tuple[Any, ...]] | None = None,
    ) -> Node:
        """
        Build a Node from the separate SQL result parts.
        """
        # Get basic fields
        title, text_source, raw_text = basic_row if basic_row is not None else 3*("",)

        # Return node object
        return Node(
            key               = key,
            title             = title,
            text_source       = text_source,
            raw_text          = raw_text,
            field_list        = MySQLNodeFieldMapper.from_rows(custom_field_rows, node_key=key),
            page_profile      = MySQLPageProfileMapper.from_row(page_profile_row, node_key=key),
            detected_concepts = MySQLConceptDetectionResultMapper.from_rows(concept_rows),
        )

    @staticmethod
    def to_basic_row(node: Node) -> dict[str, Any]:
        return {
            'object_title' : node.title,
            'text_source'  : node.text_source,
            'raw_text'     : node.raw_text,
        }

    @staticmethod
    def to_custom_field_rows(node: Node) -> list[dict[str, Any]]:
        return MySQLNodeFieldMapper.to_upsert_rows(node.field_list)

    @staticmethod
    def to_page_profile_row(node: Node) -> dict[str, Any]:
        assert node.page_profile is not None
        return MySQLPageProfileMapper.to_row(node.page_profile)

    @staticmethod
    def to_detected_concepts_rows(node: Node) -> list[dict[str, Any]]:
        return MySQLConceptDetectionResultMapper.to_upsert_rows(
            node_key          = node.key,
            text_source       = node.text_source,
            detected_concepts = node.detected_concepts,
        )

    @staticmethod
    def to_simplified_dict(node: Node) -> dict[str, Any]:
        return {
            'institution_id'    : node.key.institution_id,
            'object_type'       : node.key.object_type,
            'object_id'         : node.key.object_id,
            'object_title'      : node.title,
            'text_source'       : node.text_source,
            'raw_text'          : node.raw_text,
            'custom_fields'     : MySQLNodeFieldMapper.to_simplified_rows(node.field_list),
            'page_profile'      : (MySQLPageProfileMapper.to_row(node.page_profile) if node.page_profile is not None else {}),
            'detected_concepts' : [concept.to_json() for concept in node.detected_concepts.item_list]
        }

    @staticmethod
    def from_simplified_dict(data: dict[str, Any]) -> Node:
        key = NodeKey(
            institution_id = data['institution_id'],
            object_type    = data['object_type'],
            object_id      = data['object_id'],
        )
        return Node(
            key               = key,
            title             = str(data.get('object_title') or ""),
            text_source       = str(data.get('text_source') or ""),
            raw_text          = str(data.get('raw_text') or ""),
            field_list        = MySQLNodeFieldMapper.from_dict_list(data['custom_fields'], node_key=key),
            page_profile      = MySQLPageProfileMapper.from_row(data['page_profile'], node_key=key),
            detected_concepts = MySQLConceptDetectionResultMapper.from_rows([
                (item['concept_id'], item['score'])
                for item in data.get('detected_concepts', [])
            ]),
        )

    @staticmethod
    def to_simplified_dict_list(node_list: list[Node]) -> list[dict[str, Any]]:
        return [MySQLNodeMapper.to_simplified_dict(node) for node in node_list]

    @staticmethod
    def from_simplified_dict_list(data: list[dict[str, Any]]) -> NodeList:
        return NodeList(item_list=[MySQLNodeMapper.from_simplified_dict(item) for item in (data or [])])
