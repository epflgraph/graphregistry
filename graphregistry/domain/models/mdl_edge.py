from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field
from graphregistry.domain.models.mdl_base import EdgeKey, EdgeFieldKey

# Model definition
class EdgeField(BaseModel):
    key: EdgeFieldKey
    field_value: Any

    @classmethod
    def from_json(cls, doc_json: dict[str, Any], edge_key: EdgeKey) -> "EdgeField":
        return cls(
            key=EdgeFieldKey(
                key = edge_key,
                field_language = doc_json["field_language"],
                field_name = doc_json["field_name"]
            ),
            field_value = doc_json["field_value"]
        )

    def set_from_json(self, doc_json: dict[str, Any]) -> None:
        self.key.field_language = str(doc_json.get("field_language", self.key.field_language))
        self.key.field_name     = str(doc_json.get("field_name"    , self.key.field_name))
        self.field_value        =     doc_json.get("field_value"   , self.field_value)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    def to_dict(self) -> dict[str, Any]:
        return {
            "from_institution_id" : self.key.key.from_institution_id,
            "from_object_type"    : self.key.key.from_object_type,
            "from_object_id"      : self.key.key.from_object_id,
            "to_institution_id"   : self.key.key.to_institution_id,
            "to_object_type"      : self.key.key.to_object_type,
            "to_object_id"        : self.key.key.to_object_id,
            "context"             : self.key.key.context,
            "field_language"      : self.key.field_language,
            "field_name"          : self.key.field_name,
            "field_value"         : self.field_value
        }

    def to_simplified_dict(self) -> dict[str, Any]:
        return {
            "field_language"      : self.key.field_language,
            "field_name"          : self.key.field_name,
            "field_value"         : self.field_value
        }

# Model definition
class EdgeFieldList(BaseModel):
    field_list: list[EdgeField] = Field(default_factory=list)

    @classmethod
    def from_json(cls, json_data: list[dict[str, Any]], edge_key: EdgeKey) -> "EdgeFieldList":
        return cls(
            field_list=[
                EdgeField.from_json(field_json, edge_key=edge_key)
                for field_json in (json_data or [])
            ]
        )

    def set_from_json(self, json_data: list[dict[str, Any]], edge_key: EdgeKey) -> None:
        self.field_list = [
            EdgeField.from_json(field_json, edge_key=edge_key)
            for field_json in (json_data or [])
        ]

    def to_json(self) -> list[dict[str, Any]]:
        return [field.to_json() for field in self.field_list]

    def to_list(self) -> list[dict[str, Any]]:
        return [field.to_dict() for field in self.field_list]

    def to_simplified_list(self) -> list[dict[str, Any]]:
        return [field.to_simplified_dict() for field in self.field_list]

# Model definition
class Edge(BaseModel):
    key: EdgeKey
    field_list: EdgeFieldList = Field(default_factory=EdgeFieldList)

    @classmethod
    def from_json(cls, doc_json: dict[str, Any]) -> "Edge":
        return cls.model_validate(doc_json)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    def from_simplified_dict(self, json_data: dict[str, Any]) -> None:
        self.key = EdgeKey(
             from_institution_id = json_data["from_institution_id"],
             from_object_type    = json_data["from_object_type"],
             from_object_id      = json_data["from_object_id"],
             to_institution_id   = json_data["to_institution_id"],
             to_object_type      = json_data["to_object_type"],
             to_object_id        = json_data["to_object_id"],
             context             = json_data["context"]
        )
        self.field_list.set_from_json(json_data=json_data.get("field_list", []), edge_key=self.key)

    def to_simplified_dict(self) -> dict[str, Any]:
        return {
            "from_institution_id" : self.key.from_institution_id,
            "from_object_type"    : self.key.from_object_type,
            "from_object_id"      : self.key.from_object_id,
            "to_institution_id"   : self.key.to_institution_id,
            "to_object_type"      : self.key.to_object_type,
            "to_object_id"        : self.key.to_object_id,
            "context"             : self.key.context,
            "field_list"          : self.field_list.to_simplified_list()
        }

# Model definition
class EdgeList(BaseModel):
    edge_list: list[Edge] = Field(default_factory=list)

    @classmethod
    def from_json(cls, doc_json_list: list[dict[str, Any]]) -> "EdgeList":
        return cls(edge_list=[Edge.model_validate(doc) for doc in (doc_json_list or [])])

    def to_json(self) -> list[dict[str, Any]]:
        return self.model_dump(mode="json")["edge_list"]

    def to_simplified_dict_list(self) -> list[dict[str, Any]]:
        return [edge.to_simplified_dict() for edge in self.edge_list]
