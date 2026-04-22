# graphregistry/adapters/persistence/mysql/mappers/amp_node.py
from __future__ import annotations
from typing import Any
from graphregistry.adapters.persistence.mysql.mappers.amp_pageprofile import MySQLPageProfileMapper
from graphregistry.domain.models.entities.mdl_base import NodeFieldKey, NodeKey
from graphregistry.domain.models.entities.mdl_node import Node, NodeField, NodeFieldList
from graphregistry.domain.models.tasks.mdl_conceptdet import ConceptDetectionResult, ConceptDetectionResultList

# Class definition
class MySQLConceptDetectionResultMapper:
    """
    Maps between MySQL concept detection row shapes and domain ConceptDetectionResult / ConceptDetectionResultList.
    """

    @staticmethod
    def from_row(row: tuple[Any, ...], node_key: NodeKey) -> ConceptDetectionResult:
        """
        Expected row shape:
            (concept_id, score)
        """
        concept_id, score = row
        return ConceptDetectionResult(
            concept_id   = str(concept_id or ""),
            concept_name = None,
            score        = float(score)
        )

    @staticmethod
    def from_dict(row: dict[str, Any], node_key: NodeKey) -> ConceptDetectionResult:
        """
        Expected dict shape:
            {
                "concept_id": ...,
                "score": ...
            }
        """
        return ConceptDetectionResult(
            concept_id=str(row.get("concept_id") or ""),
            score=float(row.get("score") or 0.0)
        )

    @staticmethod
    def from_rows(rows: list[tuple[Any, ...]] | None, node_key: NodeKey) -> ConceptDetectionResultList:
        """
        Expected row shape:
            (concept_id, score)
        """
        return ConceptDetectionResultList(
            item_list=[
                MySQLConceptDetectionResultMapper.from_row(row, node_key=node_key)
                for row in (rows or [])
            ]
        )

    @staticmethod
    def from_dicts(rows: list[dict[str, Any]] | None, node_key: NodeKey) -> ConceptDetectionResultList:
        """
        Expected dict shape:
            {
                "concept_id": ...,
                "score": ...
            }
        """
        return ConceptDetectionResultList(
            item_list=[
                MySQLConceptDetectionResultMapper.from_dict(row, node_key=node_key)
                for row in (rows or [])
            ]
        )

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
