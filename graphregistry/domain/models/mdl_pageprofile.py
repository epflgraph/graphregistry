from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class PageProfile(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_json(cls, doc_json: dict[str, Any]) -> "PageProfile":
        if doc_json is None:
            return cls()
        if "values" in doc_json and isinstance(doc_json["values"], dict):
            return cls.model_validate(doc_json)
        return cls(values=doc_json)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")
