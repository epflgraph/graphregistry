# graphregistry/adapters/persistence/mysql/mappers/amp_conceptmap.py
from __future__ import annotations
from typing import Any
from graphregistry.domain.models.entities.mdl_base import NodeKey
from graphregistry.domain.models.entities.mdl_conceptmap import Concept, ScoredConcept, ScoredConceptList

# Class definition
class MySQLConceptMapper:
    """ Maps between MySQL row shapes and the domain ScoredConcept and ScoredConceptList models.
    """
    @staticmethod
    def from_row(row: tuple[Any, ...]) -> ScoredConcept:
        return ScoredConcept(concept=Concept(id=str(row[0]), name=None), score=float(row[1]))

    @staticmethod
    def from_rows(rows: list[tuple[Any, ...]] | None) -> ScoredConceptList:
        return ScoredConceptList(item_list=[MySQLConceptMapper.from_row(row) for row in (rows or [])])

    @staticmethod
    def to_upsert_row(node_key: NodeKey, text_source: str | None, scored_concept: ScoredConcept) -> dict[str, Any]:
        return {
            "object_type"    : node_key.object_type,
            "object_id"      : node_key.object_id,
            "concept_id"     : scored_concept.concept.id,
            "text_source"    : text_source,
            "score"          : scored_concept.score,
            "record_deleted" : 0,
        }

    @staticmethod
    def to_upsert_rows(node_key: NodeKey, text_source: str | None, concepts: ScoredConceptList) -> list[dict[str, Any]]:
        return [MySQLConceptMapper.to_upsert_row(node_key, text_source, concept) for concept in concepts.item_list]
