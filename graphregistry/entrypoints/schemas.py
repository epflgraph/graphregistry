# graphregistry/entrypoints/schemas.py
from __future__ import annotations
from pydantic import BaseModel, Field, StrictStr
from graphregistry.domain.types import TextLanguage, FieldLanguage, ObjectType

#==================================#
# Entrypoint specification schemas #
#==================================#

#--------------#
# Shared specs #
#--------------#

class CustomFieldSpec(BaseModel):
    field_language : FieldLanguage = "n/a"
    field_name     : str
    # Custom field values are stored as text; reject non-string inputs explicitly.
    field_value    : StrictStr

class MultilingualTextSpec(BaseModel):
    language : TextLanguage
    text     : str

#------------#
# Node specs #
#------------#

class NodeKeySpec(BaseModel):
    type : ObjectType
    id   : str

class NodeKeyListSpec(BaseModel):
    item_list: list[NodeKeySpec] = Field(default_factory=list)

class NodeSpec(BaseModel):
    type          : ObjectType
    subtype       : str | list[MultilingualTextSpec] | None = None
    id            : str
    short_code    : str | None = None
    title         : str | list[MultilingualTextSpec] | None = None
    description   : str | list[MultilingualTextSpec] | dict[str, list[MultilingualTextSpec]] | None = None
    url           : str | list[MultilingualTextSpec] | None = None
    custom_fields : list[CustomFieldSpec] | None = Field(default_factory=list)

class NodeListSpec(BaseModel):
    item_list: list[NodeSpec] = Field(default_factory=list)

#------------#
# Edge specs #
#------------#

class EdgeKeySpec(BaseModel):
    from_type : ObjectType
    from_id   : str
    to_type   : ObjectType
    to_id     : str
    context   : str

class EdgeKeyListSpec(BaseModel):
    item_list: list[EdgeKeySpec] = Field(default_factory=list)

class EdgeSpec(BaseModel):
    from_type     : ObjectType
    from_id       : str
    to_type       : ObjectType
    to_id         : str
    context       : str = Field(default="part of", description="Edge context. Defaults to 'part of' when saving edges.")
    custom_fields : list[CustomFieldSpec] | None = Field(default_factory=list)

class EdgeListSpec(BaseModel):
    item_list: list[EdgeSpec] = Field(default_factory=list)
