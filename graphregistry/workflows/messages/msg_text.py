# graphregistry/workflows/messages/msg_text.py
from __future__ import annotations
from pydantic import BaseModel
from graphregistry.domain.models.mdl_text import (
    LanguageCode,
    MultilingualGeneratedText,
)

class AutofillGeneratedTextRequest(BaseModel):
    text: MultilingualGeneratedText
    source_language: LanguageCode
    target_languages: tuple[LanguageCode, ...] = ("en", "fr", "de", "it")
    overwrite_existing: bool = False

class AutofillGeneratedTextResponse(BaseModel):
    text: MultilingualGeneratedText
