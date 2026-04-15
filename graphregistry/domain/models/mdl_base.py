# graphregistry/domain/models/mdl_base.py
from __future__ import annotations
from pydantic import BaseModel, model_validator

# Model definition
class NodeKey(BaseModel):
    model_config = {"frozen": True}
    institution_id: str
    object_type: str
    object_id: str

    @model_validator(mode="before")
    @classmethod
    def parse_tuple(cls, data):
        if isinstance(data, tuple):
            if len(data) != 3:
                raise ValueError("NodeKey tuple must have 3 elements")
            return {
                "institution_id" : data[0],
                "object_type"    : data[1],
                "object_id"      : data[2],
            }
        return data

    @classmethod
    def from_tuple(cls, value: tuple[str, str, str]) -> "NodeKey":
        return cls.model_validate(value)

    def to_tuple(self) -> tuple[str, str, str]:
        return (self.institution_id, self.object_type, self.object_id)

    def to_dict(self) -> dict[str, str]:
        return {
            "institution_id" : self.institution_id,
            "object_type"    : self.object_type,
            "object_id"      : self.object_id,
        }

# Model definition
class NodeFieldKey(BaseModel):
    model_config = {"frozen": True}
    key: NodeKey
    field_language: str = ""
    field_name: str = ""

    def to_tuple(self) -> tuple[str, str, str, str, str]:
        return (
            self.key.institution_id,
            self.key.object_type,
            self.key.object_id,
            self.field_language,
            self.field_name,
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "institution_id" : self.key.institution_id,
            "object_type"    : self.key.object_type,
            "object_id"      : self.key.object_id,
            "field_language" : self.field_language,
            "field_name"     : self.field_name,
        }

# Model definition
class EdgeKey(BaseModel):
    model_config = {"frozen": True}
    from_institution_id: str
    from_object_type: str
    from_object_id: str
    to_institution_id: str
    to_object_type: str
    to_object_id: str
    context: str

    @model_validator(mode="before")
    @classmethod
    def parse_tuple(cls, data):
        if isinstance(data, tuple):
            if len(data) != 7:
                raise ValueError("EdgeKey tuple must have 7 elements")
            return {
                "from_institution_id" : data[0],
                "from_object_type"    : data[1],
                "from_object_id"      : data[2],
                "to_institution_id"   : data[3],
                "to_object_type"      : data[4],
                "to_object_id"        : data[5],
                "context"             : data[6],
            }
        return data

    @classmethod
    def from_tuple(cls, value: tuple[str, str, str, str, str, str, str]) -> "EdgeKey":
        return cls.model_validate(value)

    def to_tuple(self) -> tuple[str, str, str, str, str, str, str]:
        return (
            self.from_institution_id,
            self.from_object_type,
            self.from_object_id,
            self.to_institution_id,
            self.to_object_type,
            self.to_object_id,
            self.context,
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "from_institution_id" : self.from_institution_id,
            "from_object_type"    : self.from_object_type,
            "from_object_id"      : self.from_object_id,
            "to_institution_id"   : self.to_institution_id,
            "to_object_type"      : self.to_object_type,
            "to_object_id"        : self.to_object_id,
            "context"             : self.context,
        }

# Model definition
class EdgeFieldKey(BaseModel):
    model_config = {"frozen": True}
    key: EdgeKey
    field_language: str
    field_name: str

    def to_tuple(self) -> tuple[str, str, str, str, str, str, str, str, str]:
        return (
            self.key.from_institution_id,
            self.key.from_object_type,
            self.key.from_object_id,
            self.key.to_institution_id,
            self.key.to_object_type,
            self.key.to_object_id,
            self.key.context,
            self.field_language,
            self.field_name,
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "from_institution_id" : self.key.from_institution_id,
            "from_object_type"    : self.key.from_object_type,
            "from_object_id"      : self.key.from_object_id,
            "to_institution_id"   : self.key.to_institution_id,
            "to_object_type"      : self.key.to_object_type,
            "to_object_id"        : self.key.to_object_id,
            "context"             : self.key.context,
            "field_language"      : self.field_language,
            "field_name"          : self.field_name,
        }