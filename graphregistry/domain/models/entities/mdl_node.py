# graphregistry/domain/models/mdl_node.py
from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field, model_validator
from graphregistry.domain.models.entities.mdl_base import NodeKey, NodeFieldKey
from graphregistry.domain.models.entities.mdl_pageprofile import PageProfile
from graphregistry.domain.models.tasks.mdl_conceptdet import ConceptDetectionResultList

# Model definition
class NodeField(BaseModel):
    """Model representing a field of a node, which consists of a key and a value.
    The key is a NodeFieldKey, which includes the node key, field name, and field language.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    key: NodeFieldKey
    field_value: Any = ""

    #------------------#
    # Model validators #
    #------------------#
    @model_validator(mode="after")
    def validate_key_consistency(self) -> "NodeField":
        if not isinstance(self.key, NodeFieldKey):
            raise TypeError("key must be a NodeFieldKey")
        return self

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, input_json: dict[str, Any], node_key: NodeKey) -> "NodeField":
        return cls(
            key=NodeFieldKey(
                key = node_key,
                field_language = str(input_json["field_language"]),
                field_name     = str(input_json["field_name"]),
            ),
            field_value=input_json["field_value"],
        )

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

# Model definition
class NodeFieldList(BaseModel):
    """Model representing a list of fields of a node.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    item_list: list[NodeField] = Field(default_factory=list)

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, input_json: list[dict[str, Any]], key: NodeKey) -> "NodeFieldList":
        return cls(
            item_list=[
                NodeField.from_json(field_json, node_key=key)
                for field_json in (input_json or [])
            ]
        )

    def to_json(self) -> list[dict[str, Any]]:
        return [field.to_json() for field in self.item_list]

# Model definition
class Node(BaseModel):
    """Model representing a graph node.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    key: NodeKey
    title: str = ""
    text_source: str = ""
    raw_text: str = ""
    item_list: NodeFieldList = Field(default_factory=NodeFieldList)
    page_profile: PageProfile | None = None
    detected_concepts: ConceptDetectionResultList = Field(default_factory=ConceptDetectionResultList)

    #------------------#
    # Model validators #
    #------------------#
    @model_validator(mode="after")
    def set_default_page_profile(self) -> "Node":
        if self.page_profile is None:
            self.page_profile = PageProfile(key=self.key)
        elif self.page_profile.key != self.key:
            self.page_profile = self.page_profile.model_copy(update={"key": self.key})
        return self

    @model_validator(mode="after")
    def validate_field_keys(self) -> "Node":
        fixed_fields: list[NodeField] = []
        for field in self.item_list.item_list:
            if field.key.key != self.key:
                field = field.model_copy(
                    update={
                        "key": field.key.model_copy(update={"key": self.key})
                    }
                )
            fixed_fields.append(field)
        self.item_list = NodeFieldList(item_list=fixed_fields)
        return self

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, input_json: dict[str, Any]) -> "Node":
        return cls.model_validate(input_json)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

# Model definition
class NodeList(BaseModel):
    """Model representing a list of graph nodes.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    item_list: list[Node] = Field(default_factory=list)

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, input_json: list[dict[str, Any]]) -> "NodeList":
        return cls(item_list=[Node.model_validate(doc) for doc in (input_json or [])])

    def to_json(self) -> list[dict[str, Any]]:
        return self.model_dump(mode='json')['item_list']
