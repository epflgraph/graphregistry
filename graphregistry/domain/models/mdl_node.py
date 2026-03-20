from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field

# Model definition
class NodeKey(BaseModel):
    institution_id: str
    object_type: str
    object_id: str

# Model definition
class NodeFieldKey(BaseModel):
    key: NodeKey
    field_language: str
    field_name: str

# Model definition
class NodeField(BaseModel):
    key: NodeFieldKey
    field_value: Any

# Model definition
class NodeFieldList(BaseModel):
    field_list: list[NodeField] = Field(default_factory=list)

# Model definition
class Node(BaseModel):
    key: NodeKey
    field_list: NodeFieldList = Field(default_factory=NodeFieldList)

    @classmethod
    def from_json(cls, doc_json: dict[str, Any]) -> "Node":
        return cls.model_validate(doc_json)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

# Model definition
class NodeList(BaseModel):
    node_list: list[Node] = Field(default_factory=list)

    @classmethod
    def from_json(cls, doc_json_list: list[dict[str, Any]]) -> "NodeList":
        return cls(node_list=[Node.model_validate(doc) for doc in (doc_json_list or [])])

    def to_json(self) -> list[dict[str, Any]]:
        return self.model_dump(mode="json")["node_list"]
