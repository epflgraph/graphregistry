# graphregistry/domain/models/mdl_concept.py
from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field
from graphregistry.domain.models.entities.mdl_base import NodeKey
import rich

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
    node_key: NodeKey | None = None
    concept_id: str
    concept_name: str
    text_source: str | None = None
    score: float

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, input_json: dict[str, Any]) -> "ConceptDetectionResult":
        return cls.model_validate(input_json)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

    #-----------------------#

    # Method: Import from simplified dictionary
    def set_from_simplified_dict(self, input_dict: dict[str, str | float]) -> "ConceptDetectionResult":
        self.node_key = NodeKey(
            institution_id = input_dict['institution_id'],
            object_type    = input_dict['object_type'],
            object_id      = input_dict['object_id']
        )
        self.concept_id   = input_dict['concept_id'],
        self.concept_name = input_dict['concept_name'],
        self.text_source  = input_dict['text_source'],
        self.score        = input_dict['score']
        return self

    # Method: Export as simplified dictionary
    def to_simplified_dict(self) -> dict[str, str | float]:
        return {
            'institution_id' : self.node_key.institution_id,
            'object_type'    : self.node_key.object_type,
            'object_id'      : self.node_key.object_id,
            'concept_id'     : self.concept_id,
            'concept_name'   : self.concept_name,
            'text_source'    : self.text_source,
            'score'          : self.score
        }

    #-----------------------#
    # Model linking methods #
    #-----------------------#

    # Method: Link current object to external Node object
    def link_to_node(self, node_key: NodeKey) -> "ConceptDetectionResult":
        self.node_key = node_key
        return self

    #----------------------#
    # Data display methods #
    #----------------------#

    # Method: Print object data in a human-readable format
    def print(self) -> None:
        rich.print_json(data=self.to_simplified_list())

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

    #-----------------------#

    # Method: Import from simplified dictionary list
    def set_from_simplified_list(self, input_list: list[dict[str, str | float]]) -> "ConceptDetectionResultList":
        self.item_list = [
            ConceptDetectionResult(
                node_key = NodeKey(
                    institution_id = dict_item['institution_id'],
                    object_type    = dict_item['object_type'],
                    object_id      = dict_item['object_id']
                ),
                concept_id   = dict_item['concept_id'],
                concept_name = dict_item['concept_name'],
                text_source  = dict_item['text_source'],
                score        = dict_item['score']
            )
            for dict_item in input_list
        ]
        return self

    # Method: Export as simplified dictionary list
    def to_simplified_list(self) -> list[dict[str, str | float]]:
        return [item.to_simplified_dict() for item in self.item_list]

    #-----------------------#
    # Model linking methods #
    #-----------------------#

    # Method: Link current object to external Node object
    def link_to_node(self, node_key: NodeKey) -> "ConceptDetectionResultList":
        for k in len(self.item_list):
            self.item_list[k].link_to_node(node_key)
        return self

    #----------------------#
    # Data display methods #
    #----------------------#

    # Method: Print object data in a human-readable format
    def print(self) -> None:
        rich.print_json(data=self.to_simplified_list())

#========================#
# Test run from terminal #
#========================#
if __name__ == "__main__":
    cdrl = ConceptDetectionResultList()
    cdrl.set_from_simplified_list(
        [
            {
                'institution_id' : 'EPFL',
                'object_type'    : 'Course',
                'object_id'      : 'PHYS-101',
                'concept_id'     : '2145243',
                'concept_name'   : 'Quantum mechanics',
                'text_source'    : 'course descritpion',
                'score'          : 0.78
            },
            {
                'institution_id' : 'EPFL',
                'object_type'    : 'Course',
                'object_id'      : 'PHYS-101',
                'concept_id'     : '5234542',
                'concept_name'   : 'Electron',
                'text_source'    : 'course descritpion',
                'score'          : 0.65
            },
            {
                'institution_id' : 'EPFL',
                'object_type'    : 'Course',
                'object_id'      : 'PHYS-101',
                'concept_id'     : '98731765',
                'concept_name'   : 'Speed of light',
                'text_source'    : 'course descritpion',
                'score'          : 0.34
            }
        ]
    )
    cdrl.print()