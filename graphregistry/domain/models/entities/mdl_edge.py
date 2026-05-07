# graphregistry/domain/models/entities/mdl_edge.py
from __future__ import annotations
from typing import Any, cast
from pydantic import BaseModel, Field
from graphregistry.domain.models.entities.mdl_base import EdgeKey, EdgeFieldKey, FieldLanguage

# Model definition
class EdgeField(BaseModel):
    """Model representing a field of an edge, which consists of a key and a value.
    The key is an EdgeFieldKey, which includes the edge key, field name, and field language.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    key: EdgeFieldKey
    field_value: Any = ""

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, json_data: dict[str, Any], edge_key: EdgeKey) -> "EdgeField":
        return cls(
            key=EdgeFieldKey(
                key = edge_key,
                field_language = cast(FieldLanguage, json_data["field_language"]),
                field_name     = str(json_data["field_name"]),
            ),
            field_value=json_data.get("field_value", ""),
        )

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

# Model definition
class EdgeFieldList(BaseModel):
    """Model representing a list of fields of an edge.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    item_list: list[EdgeField] = Field(default_factory=list)

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_list(cls, input_list: list[dict[str, Any]], key: EdgeKey) -> "EdgeFieldList":
        return cls(
            item_list=[
                EdgeField.from_json(field_json, edge_key=key)
                for field_json in (input_list or [])
            ]
        )

    def to_list(self) -> list[dict[str, Any]]:
        return [field.to_json() for field in self.item_list]

# Model definition
class Edge(BaseModel):
    """Model representing a graph edge, which connects two nodes and has a list of fields.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    key: EdgeKey
    field_list: EdgeFieldList = Field(default_factory=EdgeFieldList)

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, input_json: dict[str, Any]) -> "Edge":
        return cls.model_validate(input_json)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

# Model definition
class EdgeList(BaseModel):
    """Model representing a list of edges of a node.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    item_list: list[Edge] = Field(default_factory=list)

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_list(cls, input_list: list[dict[str, Any]]) -> "EdgeList":
        return cls(item_list=[Edge.model_validate(doc) for doc in (input_list or [])])

    def to_list(self) -> list[dict[str, Any]]:
        return [edge.to_json() for edge in self.item_list]
