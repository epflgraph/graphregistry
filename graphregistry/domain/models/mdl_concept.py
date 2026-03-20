from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field
from graphregistry.domain.models.mdl_node import NodeKey

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
