# graphregistry/adapters/persistence/mysql/mappers/amp_conceptdet.py
from __future__ import annotations

from typing import Any

from graphregistry.domain.models.entities.mdl_base import NodeKey
from graphregistry.domain.models.tasks.mdl_conceptdet import (
    ConceptDetectionResult,
    ConceptDetectionResultList,
)


class MySQLConceptDetectionResultMapper:
    """
    Maps between MySQL concept-detection row shapes and domain
    ConceptDetectionResult / ConceptDetectionResultList.

    Persistence table:
        Edges_N_Object_N_Concept_T_ConceptDetection
    """

    @staticmethod
    def from_row(row: tuple[Any, ...]) -> ConceptDetectionResult:
        """
        Expected row shape:
            (concept_id, score)
        """
        concept_id, score = row
        return ConceptDetectionResult(
            concept_id=str(concept_id),
            concept_name=None,
            score=float(score),
        )

    @staticmethod
    def from_rows(rows: list[tuple[Any, ...]] | None) -> ConceptDetectionResultList:
        """
        Expected row list shape:
            [
                (concept_id, score),
                ...
            ]
        """
        return ConceptDetectionResultList(
            item_list=[
                MySQLConceptDetectionResultMapper.from_row(row)
                for row in (rows or [])
            ]
        )

    @staticmethod
    def to_upsert_row(
        node_key: NodeKey,
        text_source: str,
        concept: ConceptDetectionResult,
    ) -> dict[str, Any]:
        """
        Returns one row suitable for upserting into
        Edges_N_Object_N_Concept_T_ConceptDetection.
        """
        return {
            "institution_id": node_key.institution_id,
            "object_type": node_key.object_type,
            "object_id": node_key.object_id,
            "concept_id": concept.concept_id,
            "text_source": text_source,
            "score": concept.score,
        }

    @staticmethod
    def to_upsert_rows(
        node_key: NodeKey,
        text_source: str,
        detected_concepts: ConceptDetectionResultList,
    ) -> list[dict[str, Any]]:
        """
        Returns rows suitable for upserting into
        Edges_N_Object_N_Concept_T_ConceptDetection.
        """
        return [
            MySQLConceptDetectionResultMapper.to_upsert_row(
                node_key=node_key,
                text_source=text_source,
                concept=concept,
            )
            for concept in detected_concepts.item_list
        ]
