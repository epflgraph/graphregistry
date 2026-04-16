# graphregistry/domain/models/mdl_pageprofile.py
from __future__ import annotations
from pydantic import BaseModel, Field
from graphregistry.domain.models.mdl_base import NodeKey
from graphregistry.domain.models.mdl_text import MultilingualText, MultilingualGeneratedText, DescriptionSet

# Model definition
class PageProfile(BaseModel):
    key          : NodeKey
    numeric_id   : MultilingualText = Field(default_factory=MultilingualText)
    short_code   : str = ""
    subtype      : MultilingualText = Field(default_factory=MultilingualText)
    name         : MultilingualGeneratedText = Field(default_factory=MultilingualGeneratedText)
    description  : DescriptionSet   = Field(default_factory=DescriptionSet)
    external_key : MultilingualText = Field(default_factory=MultilingualText)
    external_url : MultilingualText = Field(default_factory=MultilingualText)
    is_visible   : bool = True
