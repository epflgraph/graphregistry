# graphregistry/domain/models/entities/mdl_base.py
from __future__ import annotations
from pydantic import BaseModel, model_validator

# Model definition
class NodeKey(BaseModel):
    """Model representing the unique key of a node.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    model_config = {"frozen": True}
    institution_id: str
    object_type: str
    object_id: str

    #-----------------------------------#
    # Model constructors and validators #
    #-----------------------------------#
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

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_tuple(cls, input_tuple: tuple[str, str, str]) -> "NodeKey":
        return cls.model_validate(input_tuple)

    def to_tuple(self) -> tuple[str, str, str]:
        return (self.institution_id, self.object_type, self.object_id)

# Model definition
class NodeFieldKey(BaseModel):
    """Model representing the unique key of a node field.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    model_config = {"frozen": True}
    key: NodeKey
    field_language: str = ""
    field_name: str = ""

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_tuple(cls, input_tuple: tuple[str, str, str, str, str]) -> "NodeFieldKey":
        if len(input_tuple) != 5:
            raise ValueError("NodeFieldKey tuple must have 5 elements")
        node_key = NodeKey.from_tuple(input_tuple[:3])
        return cls(
            key=node_key,
            field_language=input_tuple[3],
            field_name=input_tuple[4],
        )

    def to_tuple(self) -> tuple[str, str, str, str, str]:
        return (
            self.key.institution_id,
            self.key.object_type,
            self.key.object_id,
            self.field_language,
            self.field_name,
        )

# Model definition
class EdgeKey(BaseModel):
    """Model representing the unique key of an edge.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    model_config = {"frozen": True}
    from_institution_id: str
    from_object_type: str
    from_object_id: str
    to_institution_id: str
    to_object_type: str
    to_object_id: str
    context: str

    #-----------------------------------#
    # Model constructors and validators #
    #-----------------------------------#
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

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_tuple(cls, input_tuple: tuple[str, str, str, str, str, str, str]) -> "EdgeKey":
        return cls.model_validate(input_tuple)

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

# Model definition
class EdgeFieldKey(BaseModel):
    """Model representing the unique key of an edge field.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    model_config = {"frozen": True}
    key: EdgeKey
    field_language: str
    field_name: str

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_tuple(cls, input_tuple: tuple[str, str, str, str, str, str, str, str, str]) -> "EdgeFieldKey":
        if len(input_tuple) != 9:
            raise ValueError("EdgeFieldKey tuple must have 9 elements")
        edge_key = EdgeKey.from_tuple(input_tuple[:7])
        return cls(
            key=edge_key,
            field_language=input_tuple[7],
            field_name=input_tuple[8],
        )

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
