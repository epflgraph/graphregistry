# graphregistry/domain/models/mdl_text.py
from __future__ import annotations
from typing import Literal, TypeAlias
from pydantic import BaseModel, Field

# Type alias for language codes
LanguageCode: TypeAlias = Literal["en", "fr", "de", "it"]

# Model definition
class MultilingualText(BaseModel):
    en: str = ""
    fr: str = ""
    de: str = ""
    it: str = ""

# Model definition
class GeneratedText(BaseModel):
    is_auto_generated:  bool = False
    is_auto_corrected:  bool = False
    is_auto_translated: bool = False
    translated_from: LanguageCode | None = None
    value: str = ""

# Model definition
class MultilingualGeneratedText(BaseModel):
    en: GeneratedText = Field(default_factory=GeneratedText)
    fr: GeneratedText = Field(default_factory=GeneratedText)
    de: GeneratedText = Field(default_factory=GeneratedText)
    it: GeneratedText = Field(default_factory=GeneratedText)

# Model definition
class DescriptionSet(BaseModel):
    short:  MultilingualGeneratedText = Field(default_factory=MultilingualGeneratedText)
    medium: MultilingualGeneratedText = Field(default_factory=MultilingualGeneratedText)
    long:   MultilingualGeneratedText = Field(default_factory=MultilingualGeneratedText)
