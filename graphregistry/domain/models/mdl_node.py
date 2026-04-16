
# graphregistry/domain/models/mdl_node.py
from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field, model_validator
from graphregistry.domain.models.mdl_base import NodeKey, NodeFieldKey
from graphregistry.domain.models.mdl_pageprofile import PageProfile

# Model definition
class NodeField(BaseModel):
    key: NodeFieldKey
    field_value: Any = ""

    @classmethod
    def from_json(cls, json_data: dict[str, Any], node_key: NodeKey) -> "NodeField":
        return cls(
            key=NodeFieldKey(
                key = node_key,
                field_language = json_data["field_language"],
                field_name = json_data["field_name"]
            ),
            field_value = json_data["field_value"]
        )

    def set_from_json(self, json_data: dict[str, Any]) -> None:
        self.key.field_language = str(json_data.get("field_language", self.key.field_language))
        self.key.field_name     = str(json_data.get("field_name"    , self.key.field_name))
        self.field_value        =     json_data.get("field_value"   , self.field_value)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    def to_dict(self) -> dict[str, Any]:
        return {
            "institution_id" : self.key.key.institution_id,
            "object_type"    : self.key.key.object_type,
            "object_id"      : self.key.key.object_id,
            "field_language" : self.key.field_language,
            "field_name"     : self.key.field_name,
            "field_value"    : self.field_value
        }

# Model definition
class NodeFieldList(BaseModel):
    field_list: list[NodeField] = Field(default_factory=list)

    @classmethod
    def from_json(cls, data: list[dict[str, Any]], key: NodeKey) -> "NodeFieldList":
        return cls(
            field_list=[
                NodeField.from_json(field_json, node_key=key)
                for field_json in (data or [])
            ]
        )

    def set_from_list(self, json_data: list[dict[str, Any]], node_key: NodeKey) -> None:
        self.field_list = [
            NodeField.from_json(field_json, node_key=node_key)
            for field_json in (json_data or [])
        ]

    def to_json(self) -> list[dict[str, Any]]:
        return [field.to_json() for field in self.field_list]

    def to_list(self) -> list[dict[str, Any]]:
        return [field.to_dict() for field in self.field_list]

# Model definition
class Node(BaseModel):
    key          : NodeKey
    title        : str = ""
    text_source  : str = ""
    raw_text     : str = ""
    field_list   : NodeFieldList = Field(default_factory=NodeFieldList)
    page_profile : PageProfile | None = None

    @model_validator(mode="after")
    def set_default_page_profile(self) -> "Node":
        if self.page_profile is None:
            self.page_profile = PageProfile(key=self.key)
        elif self.page_profile.key != self.key:
            self.page_profile = self.page_profile.model_copy(update={"key": self.key})
        return self

    @classmethod
    def from_json(cls, json_data: dict[str, Any]) -> "Node":
        return cls.model_validate(json_data)

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
