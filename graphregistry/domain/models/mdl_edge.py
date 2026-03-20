from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field

# Model definition
class EdgeKey(BaseModel):
    from_institution_id: str
    from_object_type: str
    from_object_id: str
    to_institution_id: str
    to_object_type: str
    to_object_id: str
    context: str

# Model definition
class EdgeFieldKey(BaseModel):
    key: EdgeKey
    field_language: str
    field_name: str

# Model definition
class EdgeField(BaseModel):
    key: EdgeFieldKey
    field_value: Any

# Model definition
class EdgeFieldList(BaseModel):
    field_list: list[EdgeField] = Field(default_factory=list)

# Model definition
class Edge(BaseModel):
    key: EdgeKey
    field_list: EdgeFieldList = Field(default_factory=EdgeFieldList)

    @classmethod
    def from_json(cls, doc_json: dict[str, Any]) -> "Edge":
        return cls.model_validate(doc_json)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

# Model definition
class EdgeList(BaseModel):
    edge_list: list[Edge] = Field(default_factory=list)

    @classmethod
    def from_json(cls, doc_json_list: list[dict[str, Any]]) -> "EdgeList":
        return cls(edge_list=[Edge.model_validate(doc) for doc in (doc_json_list or [])])

    def to_json(self) -> list[dict[str, Any]]:
        return self.model_dump(mode="json")["edge_list"]
