# graphregistry/domain/models/entities/mdl_pageprofile.py
from __future__ import annotations
from pydantic import BaseModel, Field, model_validator
from graphregistry.domain.models.entities.mdl_base import NodeKey
from graphregistry.domain.models.entities.mdl_text import DescriptionSet, MultilingualGeneratedText, MultilingualText

# Model definition
class PageProfile(BaseModel):
    """Model representing the profile of a page, which is a type of node in the graph.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    key          : NodeKey
    numeric_id   : MultilingualText = Field(default_factory=MultilingualText)
    short_code   : str = ""
    subtype      : MultilingualText = Field(default_factory=MultilingualText)
    name         : MultilingualGeneratedText = Field(default_factory=MultilingualGeneratedText)
    description  : DescriptionSet   = Field(default_factory=DescriptionSet)
    external_key : MultilingualText = Field(default_factory=MultilingualText)
    external_url : MultilingualText = Field(default_factory=MultilingualText)
    is_visible   : bool = True

    #-----------------------------------#
    # Model constructors and validators #
    #-----------------------------------#
    @model_validator(mode="after")
    def validate_key_type(self) -> "PageProfile":
        if not isinstance(self.key, NodeKey):
            raise TypeError("key must be a NodeKey")
        return self

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, input_json: dict) -> "PageProfile":
        return cls.model_validate(input_json)

    def to_json(self) -> dict:
        return self.model_dump(mode="json")
