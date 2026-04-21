# graphregistry/domain/models/mdl_concept.py
from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field

# Model definition
class ConceptDetectionTask(BaseModel):
    """Task model representing a concept detection operation
    """
    #--------------------#
    # Internal variables #
    #--------------------#
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

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, json_input: dict[str, Any]) -> "ConceptDetectionTask":
        return cls.model_validate(json_input)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

    #-----------------------#

    # Method: Get payload dictionary for task execution
    def get_payload_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.text is not None:
            payload["raw_text"] = self.text
        if self.keywords is not None:
            payload["keywords"] = self.keywords
        return payload

    # Method: Get parameters dictionary for task execution
    def get_params_dict(self) -> dict[str, Any]:
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
class ConceptDetectionResult(BaseModel):
    """Model representing an object-to-concept detection result
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    concept_id: str
    concept_name: str
    score: float

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, input_json: dict[str, Any]) -> "ConceptDetectionResult":
        return cls.model_validate(input_json)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

# Model definition
class ConceptDetectionResultList(BaseModel):
    """Model representing a list of object-to-concept detection results
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    item_list: list[ConceptDetectionResult] = Field(default_factory=list)

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, input_json: list[dict[str, Any]]) -> "ConceptDetectionResultList":
        return cls(item_list=[ConceptDetectionResult.model_validate(doc) for doc in (input_json or [])])

    def to_json(self) -> list[dict[str, Any]]:
        return self.model_dump(mode='json')['item_list']
