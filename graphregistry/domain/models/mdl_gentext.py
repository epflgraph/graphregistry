from __future__ import annotations
from pydantic import BaseModel, Field

# Model definition
class MultilingualText(BaseModel):
    en: str | None = None
    fr: str | None = None
    de: str | None = None
    it: str | None = None

# Model definition
class GeneratedText(BaseModel):
    is_auto_generated:  bool | None = None
    is_auto_corrected:  bool | None = None
    is_auto_translated: bool | None = None
    translated_from: str | None = None
    value: str | None = None

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
