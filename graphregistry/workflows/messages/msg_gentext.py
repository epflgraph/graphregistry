from __future__ import annotations

from pydantic import BaseModel

from graphregistry.domain.models.mdl_gentext import (
    GeneratedText,
    MultilingualGeneratedText,
    MultilingualText,
)


class TranslateMultilingualTextRequest(BaseModel):
    text: MultilingualText
    source_language: str
    target_languages: tuple[str, ...] = ("en", "fr", "de", "it")


class TranslateMultilingualTextResponse(BaseModel):
    text: MultilingualText


class GenerateTextRequest(BaseModel):
    prompt: str
    language: str = "en"


class GenerateTextResponse(BaseModel):
    text: GeneratedText


class GenerateAndTranslateRequest(BaseModel):
    prompt: str
    source_language: str = "en"
    target_languages: tuple[str, ...] = ("en", "fr", "de", "it")


class GenerateAndTranslateResponse(BaseModel):
    text: MultilingualGeneratedText
