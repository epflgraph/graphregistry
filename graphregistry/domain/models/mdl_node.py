# graphregistry/domain/models/mdl_node.py
from __future__ import annotations
from typing import Any, Iterator
from pydantic import BaseModel, Field, model_validator
from graphregistry.domain.models.mdl_base import NodeKey, NodeFieldKey
from graphregistry.domain.models.mdl_pageprofile import PageProfile
from graphregistry.domain.models.mdl_concept import DetectedConceptList, DetectedConcept

# Model definition
class NodeField(BaseModel):
    key: NodeFieldKey
    field_value: Any = ""

    @model_validator(mode="after")
    def validate_key_consistency(self) -> "NodeField":
        if not isinstance(self.key, NodeFieldKey):
            raise TypeError("key must be a NodeFieldKey")
        return self

    @classmethod
    def from_json(cls, json_data: dict[str, Any], node_key: NodeKey) -> "NodeField":
        return cls(
            key=NodeFieldKey(
                key=node_key,
                field_language=str(json_data["field_language"]),
                field_name=str(json_data["field_name"]),
            ),
            field_value=json_data["field_value"],
        )

    def set_from_json(self, json_data: dict[str, Any]) -> None:
        self.key = NodeFieldKey(
            key=self.key.key,
            field_language=str(json_data.get("field_language", self.key.field_language)),
            field_name=str(json_data.get("field_name", self.key.field_name)),
        )
        self.field_value = json_data.get("field_value", self.field_value)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    def to_dict(self) -> dict[str, Any]:
        return {
            "institution_id": self.key.key.institution_id,
            "object_type": self.key.key.object_type,
            "object_id": self.key.key.object_id,
            "field_language": self.key.field_language,
            "field_name": self.key.field_name,
            "field_value": self.field_value,
        }

    def matches(self, field_name: str, field_language: str | None = None) -> bool:
        if self.key.field_name != field_name:
            return False
        if field_language is not None and self.key.field_language != field_language:
            return False
        return True


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

    def iter_fields(self) -> Iterator[NodeField]:
        return iter(self.field_list)

    def __len__(self) -> int:
        return len(self.field_list)

    def __bool__(self) -> bool:
        return bool(self.field_list)

    def append(self, field: NodeField) -> None:
        self.field_list.append(field)

    def extend(self, fields: list[NodeField]) -> None:
        self.field_list.extend(fields)

    def get(self, field_name: str, field_language: str | None = None) -> NodeField | None:
        for field in self.field_list:
            if field.matches(field_name=field_name, field_language=field_language):
                return field
        return None

    def get_value(self, field_name: str, field_language: str | None = None, default: Any = None) -> Any:
        field = self.get(field_name=field_name, field_language=field_language)
        return field.field_value if field is not None else default

    def filter(self, field_name: str | None = None, field_language: str | None = None) -> list[NodeField]:
        out: list[NodeField] = []
        for field in self.field_list:
            if field_name is not None and field.key.field_name != field_name:
                continue
            if field_language is not None and field.key.field_language != field_language:
                continue
            out.append(field)
        return out

    def upsert(self, field: NodeField) -> None:
        for i, existing in enumerate(self.field_list):
            if existing.key == field.key:
                self.field_list[i] = field
                return
        self.field_list.append(field)

    def remove(self, field_name: str, field_language: str | None = None) -> int:
        before = len(self.field_list)
        self.field_list = [
            field
            for field in self.field_list
            if not field.matches(field_name=field_name, field_language=field_language)
        ]
        return before - len(self.field_list)


# Model definition
class Node(BaseModel):
    key: NodeKey
    title: str = ""
    text_source: str = ""
    raw_text: str = ""
    field_list: NodeFieldList = Field(default_factory=NodeFieldList)
    page_profile: PageProfile | None = None
    detected_concepts: DetectedConceptList = Field(default_factory=DetectedConceptList)

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
        for field in self.field_list.field_list:
            if field.key.key != self.key:
                field = field.model_copy(
                    update={
                        "key": field.key.model_copy(update={"key": self.key})
                    }
                )
            fixed_fields.append(field)
        self.field_list = NodeFieldList(field_list=fixed_fields)
        return self

    @classmethod
    def from_json(cls, json_data: dict[str, Any]) -> "Node":
        return cls.model_validate(json_data)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    def has_field(self, field_name: str, field_language: str | None = None) -> bool:
        return self.field_list.get(field_name=field_name, field_language=field_language) is not None

    def get_field(self, field_name: str, field_language: str | None = None) -> NodeField | None:
        return self.field_list.get(field_name=field_name, field_language=field_language)

    def get_field_value(self, field_name: str, field_language: str | None = None, default: Any = None) -> Any:
        return self.field_list.get_value(
            field_name=field_name,
            field_language=field_language,
            default=default,
        )

    def set_field_value(self, field_name: str, field_value: Any, field_language: str = "") -> None:
        field = NodeField(
            key=NodeFieldKey(
                key=self.key,
                field_language=field_language,
                field_name=field_name,
            ),
            field_value=field_value,
        )
        self.field_list.upsert(field)

    def remove_field(self, field_name: str, field_language: str | None = None) -> int:
        return self.field_list.remove(field_name=field_name, field_language=field_language)

    def iter_fields(self) -> Iterator[NodeField]:
        return iter(self.field_list.field_list)


# Model definition
class NodeList(BaseModel):
    node_list: list[Node] = Field(default_factory=list)

    @classmethod
    def from_json(cls, doc_json_list: list[dict[str, Any]]) -> "NodeList":
        return cls(node_list=[Node.model_validate(doc) for doc in (doc_json_list or [])])

    def to_json(self) -> list[dict[str, Any]]:
        return self.model_dump(mode="json")["node_list"]

    def iter_nodes(self) -> Iterator[Node]:
        return iter(self.node_list)

    def __len__(self) -> int:
        return len(self.node_list)

    def __bool__(self) -> bool:
        return bool(self.node_list)

    def append(self, node: Node) -> None:
        self.node_list.append(node)

    def extend(self, nodes: list[Node]) -> None:
        self.node_list.extend(nodes)

    def get(self, key: NodeKey) -> Node | None:
        for node in self.node_list:
            if node.key == key:
                return node
        return None

    def keys(self) -> list[NodeKey]:
        return [node.key for node in self.node_list]
