# graphregistry/domain/models/entities/mdl_concept.py
from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field

# Model definition
class Concept(BaseModel):
    """Model representing a concept in the graph registry
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    concept_id   : str | None = None
    concept_name : str | None = None

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, json_input: dict[str, Any]) -> "Concept":
        return cls.model_validate(json_input)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

# Model definition
class ConceptList(BaseModel):
    """Model representing a list of concepts in the graph registry
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    item_list: list[Concept] = Field(default_factory=list)

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, json_input: dict[str, Any]) -> "ConceptList":
        return cls.model_validate(json_input)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

# Model definition
class ScoredConcept(BaseModel):
    """Model representing an object-to-concept detection result
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    concept_id   : str   | None = None
    concept_name : str   | None = None
    score        : float | None = None

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, input_json: dict[str, Any]) -> "ScoredConcept":
        return cls.model_validate(input_json)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

# Model definition
class ScoredConceptList(BaseModel):
    """Model representing a list of object-to-concept detection results
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    item_list: list[ScoredConcept] = Field(default_factory=list)

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_list(cls, input_list: list[dict[str, Any]]) -> "ScoredConceptList":
        return cls(item_list=[ScoredConcept.model_validate(doc) for doc in (input_list or [])])

    def to_list(self) -> list[dict[str, Any]]:
        return self.model_dump(mode='json')['item_list']
