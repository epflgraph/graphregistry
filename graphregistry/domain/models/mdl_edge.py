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

    @staticmethod
    def get_schema(from_object_type: str, to_object_type: str) -> str:
        raise NotImplementedError("Edge.get_schema is not implemented yet.")

    def _get_schema(self) -> str:
        raise NotImplementedError("Edge._get_schema is not implemented yet.")

    def exists(self) -> bool:
        raise NotImplementedError("Edge.exists is not implemented yet.")

    def info(self) -> dict[str, Any]:
        raise NotImplementedError("Edge.info is not implemented yet.")

    def set(self, *, key: EdgeKey | None = None, custom_fields: list[dict[str, Any]] | None = None) -> None:
        raise NotImplementedError("Edge.set is not implemented yet.")

    def set_from_existing(self) -> None:
        raise NotImplementedError("Edge.set_from_existing is not implemented yet.")

    def set_from_json(self, doc_json: dict[str, Any]) -> None:
        raise NotImplementedError("Edge.set_from_json is not implemented yet.")

    def commit_edge_object(self, actions: tuple[str, ...] = ("eval",)) -> Any:
        raise NotImplementedError("Edge.commit_edge_object is not implemented yet.")

    def commit_custom_fields(self, actions: tuple[str, ...] = ("eval",)) -> Any:
        raise NotImplementedError("Edge.commit_custom_fields is not implemented yet.")

    def commit(self, actions: tuple[str, ...] = ("eval",), verbose: bool = False) -> Any:
        raise NotImplementedError("Edge.commit is not implemented yet.")

# Model definition
class EdgeList(BaseModel):
    edge_list: list[Edge] = Field(default_factory=list)

    @classmethod
    def from_json(cls, doc_json_list: list[dict[str, Any]]) -> "EdgeList":
        return cls(edge_list=[Edge.model_validate(doc) for doc in (doc_json_list or [])])

    def to_json(self) -> list[dict[str, Any]]:
        return self.model_dump(mode="json")["edge_list"]

    def exists(self) -> list[bool]:
        raise NotImplementedError("EdgeList.exists is not implemented yet.")

    def info(self) -> list[dict[str, Any]]:
        raise NotImplementedError("EdgeList.info is not implemented yet.")

    def set_from_json(self, doc_json_list: list[dict[str, Any]] | None = None) -> None:
        raise NotImplementedError("EdgeList.set_from_json is not implemented yet.")

    def commit(self, actions: tuple[str, ...] = ("eval",)) -> Any:
        raise NotImplementedError("EdgeList.commit is not implemented yet.")
