# graphregistry/domain/models/tasks/mdl_conceptdet.py
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
    text                      : str | None = None
    keywords                  : list[str] | None = None
    restrict_to_ontology      : bool = False
    graph_score_smoothing     : bool = True
    ontology_score_smoothing  : bool = True
    keywords_score_smoothing  : bool = True
    normalisation_coefficient : float = 0.5
    aggregation_coef          : float = 0.5
    filtering_threshold       : float = 0.15
    filtering_min_votes       : int = 5
    refresh_scores            : bool = True
    result                    : list[dict] | None = None
    successful                : bool = False
    error_message             : str | None = None

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
            "graph_score_smoothing": self.graph_score_smoothing,
            "ontology_score_smoothing": self.ontology_score_smoothing,
            "keywords_score_smoothing": self.keywords_score_smoothing,
            "normalisation_coef": self.normalisation_coefficient,
            "aggregation_coef": self.aggregation_coef,
            "filtering_threshold": self.filtering_threshold,
            "filtering_min_votes": self.filtering_min_votes,
            "refresh_scores": self.refresh_scores,
        }

    def get_url_safe_params_dict(self) -> dict[str, str]:
        """
        Return parameters as URL query-string values.

        urlencode emits Python-style booleans ("True"/"False"). FastAPI accepts
        those, but we normalize to lowercase JSON-style booleans for clarity and
        to match the legacy client's observed URLs.
        """
        params = self.get_params_dict()
        return {
            key: ("true" if value else "false") if isinstance(value, bool) else str(value)
            for key, value in params.items()
        }
