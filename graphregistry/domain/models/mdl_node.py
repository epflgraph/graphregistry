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

    def exists(self) -> bool:
        raise NotImplementedError("Node.exists is not implemented yet.")

    def info(self) -> dict[str, Any]:
        raise NotImplementedError("Node.info is not implemented yet.")

    def set(
        self,
        *,
        key: NodeKey | None = None,
        object_title: str | None = None,
        text_source: str | None = None,
        raw_text: str | None = None,
        custom_fields: list[dict[str, Any]] | None = None,
        page_profile: dict[str, Any] | None = None,
        detect_concepts: bool = False,
    ) -> None:
        raise NotImplementedError("Node.set is not implemented yet.")

    def set_from_existing(self) -> None:
        raise NotImplementedError("Node.set_from_existing is not implemented yet.")

    def set_from_json(self, doc_json: dict[str, Any], detect_concepts: bool = False) -> None:
        raise NotImplementedError("Node.set_from_json is not implemented yet.")

    def commit_node_object(self, actions: tuple[str, ...] = ("eval",)) -> Any:
        raise NotImplementedError("Node.commit_node_object is not implemented yet.")

    def commit_custom_fields(self, actions: tuple[str, ...] = ("eval",)) -> Any:
        raise NotImplementedError("Node.commit_custom_fields is not implemented yet.")

    def commit_page_profile(self, actions: tuple[str, ...] = ("eval",)) -> Any:
        raise NotImplementedError("Node.commit_page_profile is not implemented yet.")

    def commit(self, actions: tuple[str, ...] = ("eval",), verbose: bool = False) -> Any:
        raise NotImplementedError("Node.commit is not implemented yet.")

    def detect_concepts(self) -> Any:
        raise NotImplementedError("Node.detect_concepts is not implemented yet.")

    def commit_concepts(self, actions: tuple[str, ...] = ("eval",), delete_existing: bool = False) -> Any:
        raise NotImplementedError("Node.commit_concepts is not implemented yet.")

    def refine_concepts(self) -> Any:
        raise NotImplementedError("Node.refine_concepts is not implemented yet.")

    def commit_manual_mapping(self, actions: tuple[str, ...] = ("eval",), delete_existing: bool = False) -> Any:
        raise NotImplementedError("Node.commit_manual_mapping is not implemented yet.")

# Model definition
class NodeList(BaseModel):
    node_list: list[Node] = Field(default_factory=list)

    @classmethod
    def from_json(cls, doc_json_list: list[dict[str, Any]]) -> "NodeList":
        return cls(node_list=[Node.model_validate(doc) for doc in (doc_json_list or [])])

    def to_json(self) -> list[dict[str, Any]]:
        return self.model_dump(mode="json")["node_list"]

    def exists(self) -> list[bool]:
        raise NotImplementedError("NodeList.exists is not implemented yet.")

    def info(self) -> list[dict[str, Any]]:
        raise NotImplementedError("NodeList.info is not implemented yet.")

    def set_from_json(
        self,
        doc_json_list: list[dict[str, Any]] | None = None,
        detect_concepts: bool = False,
    ) -> None:
        raise NotImplementedError("NodeList.set_from_json is not implemented yet.")

    def commit(self, actions: tuple[str, ...] = ("eval",)) -> Any:
        raise NotImplementedError("NodeList.commit is not implemented yet.")
