# graphregistry/workflows/messages/msg_text.py
from __future__ import annotations
from pydantic import BaseModel
from graphregistry.domain.models.mdl_text import (
    GeneratedText,
    LanguageCode,
    MultilingualGeneratedText,
    MultilingualText,
)

# Class definition
class TranslateMultilingualTextRequest(BaseModel):
    text: MultilingualText
    source_language: LanguageCode
    target_languages: tuple[LanguageCode, ...] = ("en", "fr", "de", "it")

# Class definition
class TranslateMultilingualTextResponse(BaseModel):
    text: MultilingualText

# Class definition
class GenerateTextRequest(BaseModel):
    prompt: str
    language: LanguageCode = "en"

# Class definition
class GenerateTextResponse(BaseModel):
    text: GeneratedText

# Class definition
class GenerateAndTranslateRequest(BaseModel):
    prompt: str
    source_language: LanguageCode = "en"
    target_languages: tuple[LanguageCode, ...] = ("en", "fr", "de", "it")

# Class definition
class GenerateAndTranslateResponse(BaseModel):
    text: MultilingualGeneratedText