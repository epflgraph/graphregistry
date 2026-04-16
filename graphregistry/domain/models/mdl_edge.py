# graphregistry/domain/models/mdl_edge.py
from __future__ import annotations
from typing import Any, Iterator
from pydantic import BaseModel, Field
from graphregistry.domain.models.mdl_base import EdgeKey, EdgeFieldKey

# Model definition
class EdgeField(BaseModel):
    key: EdgeFieldKey
    field_value: Any = ""

    @classmethod
    def from_json(cls, json_data: dict[str, Any], edge_key: EdgeKey) -> "EdgeField":
        return cls(
            key=EdgeFieldKey(
                key=edge_key,
                field_language=str(json_data["field_language"]),
                field_name=str(json_data["field_name"]),
            ),
            field_value=json_data.get("field_value", ""),
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EdgeField":
        return cls(
            key=EdgeFieldKey(
                key=EdgeKey(
                    from_institution_id=str(data["from_institution_id"]),
                    from_object_type=str(data["from_object_type"]),
                    from_object_id=str(data["from_object_id"]),
                    to_institution_id=str(data["to_institution_id"]),
                    to_object_type=str(data["to_object_type"]),
                    to_object_id=str(data["to_object_id"]),
                    context=str(data["context"]),
                ),
                field_language=str(data.get("field_language", "")),
                field_name=str(data.get("field_name", "")),
            ),
            field_value=data.get("field_value", ""),
        )

    def set_from_json(self, json_data: dict[str, Any]) -> None:
        self.key = self.key.model_copy(
            update={
                "field_language": str(json_data.get("field_language", self.key.field_language)),
                "field_name": str(json_data.get("field_name", self.key.field_name)),
            }
        )
        self.field_value = json_data.get("field_value", self.field_value)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    def to_dict(self) -> dict[str, Any]:
        return {
            "from_institution_id": self.key.key.from_institution_id,
            "from_object_type": self.key.key.from_object_type,
            "from_object_id": self.key.key.from_object_id,
            "to_institution_id": self.key.key.to_institution_id,
            "to_object_type": self.key.key.to_object_type,
            "to_object_id": self.key.key.to_object_id,
            "context": self.key.key.context,
            "field_language": self.key.field_language,
            "field_name": self.key.field_name,
            "field_value": self.field_value,
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

    @classmethod
    def from_dicts(cls, data: list[dict[str, Any]]) -> "EdgeFieldList":
        return cls(
            field_list=[
                EdgeField.from_dict(field_data)
                for field_data in (data or [])
            ]
        )

    def set_from_list(self, json_data: list[dict[str, Any]], edge_key: EdgeKey) -> None:
        self.field_list = [
            EdgeField.from_json(field_json, edge_key=edge_key)
            for field_json in (json_data or [])
        ]

    def append(self, field: EdgeField) -> None:
        self.field_list.append(field)

    def extend(self, fields: list[EdgeField]) -> None:
        self.field_list.extend(fields)

    def get(self, field_language: str, field_name: str) -> EdgeField | None:
        for field in self.field_list:
            if field.key.field_language == field_language and field.key.field_name == field_name:
                return field
        return None

    def exists(self, field_language: str, field_name: str) -> bool:
        return self.get(field_language=field_language, field_name=field_name) is not None

    def to_json(self) -> list[dict[str, Any]]:
        return [field.to_json() for field in self.field_list]

    def to_list(self) -> list[dict[str, Any]]:
        return [field.to_dict() for field in self.field_list]

    def iter_fields(self) -> Iterator[EdgeField]:
        return iter(self.field_list)

    def __len__(self) -> int:
        return len(self.field_list)

    def __bool__(self) -> bool:
        return bool(self.field_list)


# Model definition
class Edge(BaseModel):
    key: EdgeKey
    field_list: EdgeFieldList = Field(default_factory=EdgeFieldList)

    @classmethod
    def from_json(cls, json_data: dict[str, Any]) -> "Edge":
        return cls.model_validate(json_data)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Edge":
        key = EdgeKey(
            from_institution_id=str(data["from_institution_id"]),
            from_object_type=str(data["from_object_type"]),
            from_object_id=str(data["from_object_id"]),
            to_institution_id=str(data["to_institution_id"]),
            to_object_type=str(data["to_object_type"]),
            to_object_id=str(data["to_object_id"]),
            context=str(data["context"]),
        )

        return cls(
            key=key,
            field_list=EdgeFieldList.from_json(data.get("field_list", []), edge_key=key),
        )

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    def to_dict(self) -> dict[str, Any]:
        return {
            "from_institution_id": self.key.from_institution_id,
            "from_object_type": self.key.from_object_type,
            "from_object_id": self.key.from_object_id,
            "to_institution_id": self.key.to_institution_id,
            "to_object_type": self.key.to_object_type,
            "to_object_id": self.key.to_object_id,
            "context": self.key.context,
            "field_list": self.field_list.to_list(),
        }

    def get_field(self, field_language: str, field_name: str) -> EdgeField | None:
        return self.field_list.get(field_language=field_language, field_name=field_name)

    def has_field(self, field_language: str, field_name: str) -> bool:
        return self.field_list.exists(field_language=field_language, field_name=field_name)


# Model definition
class EdgeList(BaseModel):
    edge_list: list[Edge] = Field(default_factory=list)

    @classmethod
    def from_json(cls, doc_json_list: list[dict[str, Any]]) -> "EdgeList":
        return cls(edge_list=[Edge.model_validate(doc) for doc in (doc_json_list or [])])

    @classmethod
    def from_dicts(cls, data: list[dict[str, Any]]) -> "EdgeList":
        return cls(edge_list=[Edge.from_dict(item) for item in (data or [])])

    def append(self, edge: Edge) -> None:
        self.edge_list.append(edge)

    def extend(self, edges: list[Edge]) -> None:
        self.edge_list.extend(edges)

    def to_json(self) -> list[dict[str, Any]]:
        return self.model_dump(mode="json")["edge_list"]

    def to_list(self) -> list[dict[str, Any]]:
        return [edge.to_dict() for edge in self.edge_list]

    def iter_edges(self) -> Iterator[Edge]:
        return iter(self.edge_list)

    def __len__(self) -> int:
        return len(self.edge_list)

    def __bool__(self) -> bool:
        return bool(self.edge_list)
