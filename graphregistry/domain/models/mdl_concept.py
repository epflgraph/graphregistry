# graphregistry/domain/models/mdl_concept.py
from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field
from graphregistry.domain.models.mdl_base import NodeKey
import rich

class ConceptExtractionTask(BaseModel):
    text: str | None = None
    keywords: list[str] | None = None

    restrict_to_ontology: bool = False
    graph_score_smoothing: bool = True
    ontology_score_smoothing: bool = True
    keywords_score_smoothing: bool = True
    normalisation_coefficient: float = 0.5
    aggregation_coef: float = 0.5
    filtering_threshold: float = 0.15
    filtering_min_votes: int = 5
    refresh_scores: bool = True

    result: list[dict] | None = None
    successful: bool = False
    error_message: str | None = None

    def to_wikify_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.text is not None:
            payload["raw_text"] = self.text
        if self.keywords is not None:
            payload["keywords"] = self.keywords
        return payload

    def to_wikify_query_params(self) -> dict[str, Any]:
        return {
            "restrict_to_ontology": self.restrict_to_ontology,
            # "graph_score_smoothing": self.graph_score_smoothing,
            # "ontology_score_smoothing": self.ontology_score_smoothing,
            # "keywords_score_smoothing": self.keywords_score_smoothing,
            # "normalisation_coef": self.normalisation_coefficient,
            # "aggregation_coef": self.aggregation_coef,
            # "filtering_threshold": self.filtering_threshold,
            # "filtering_min_votes": self.filtering_min_votes,
            # "refresh_scores": self.refresh_scores,
        }

# Model definition
class DetectedConcept(BaseModel):
    object_key: NodeKey | None = None
    concept_id: str
    text_source: str | None = None
    score: float

    @classmethod
    def from_json(cls, doc_json: dict[str, Any]) -> "DetectedConcept":
        return cls.model_validate(doc_json)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

# Model definition
class DetectedConceptList(BaseModel):
    concept_list: list[DetectedConcept] = Field(default_factory=list)

    @classmethod
    def from_json(cls, doc_json_list: list[dict[str, Any]]) -> "DetectedConceptList":
        return cls(concept_list=[DetectedConcept.model_validate(doc) for doc in (doc_json_list or [])])

    def to_json(self) -> list[dict[str, Any]]:
        return self.model_dump(mode="json")["concept_list"]

    def link_to_node(self, node_key: NodeKey) -> "DetectedConceptList":
        return DetectedConceptList(
            concept_list=[
                concept.model_copy(update={"object_key": node_key})
                for concept in self.concept_list
            ]
        )

    def print_json(self) -> None:
        rich.print_json(data=self.to_json())

    def __str__(self) -> str:
        return f"DetectedConceptList(n={len(self.concept_list)})"