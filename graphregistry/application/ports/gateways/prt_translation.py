# graphregistry/application/ports/gateways/prt_translation.py
from __future__ import annotations
from typing import Protocol
from graphregistry.domain.types import DEFAULT_LANGUAGE_CODES
from graphregistry.domain.models.entities.mdl_text import (
    LanguageCode,
    LanguageCodeList,
    MultilingualGeneratedText,
    MultilingualText,
)

# Model definition
class TextTranslationGateway(Protocol):
    def translate_text(self, text: str, source_language: LanguageCode, target_language: LanguageCode) -> str:
        ...

    def translate_multilingual(
        self,
        text: MultilingualText,
        source_language: LanguageCode,
        target_languages: LanguageCodeList = DEFAULT_LANGUAGE_CODES,
    ) -> MultilingualText:
        ...
