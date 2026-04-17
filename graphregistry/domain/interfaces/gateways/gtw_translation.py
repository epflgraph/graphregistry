# graphregistry/domain/interfaces/gateways/gtw_translation.py
from __future__ import annotations
from typing import Protocol
from graphregistry.domain.models.mdl_text import LanguageCode, MultilingualText, MultilingualGeneratedText

# Model definition
class TextTranslationGateway(Protocol):
    def translate_text(self, text: str, source_language: LanguageCode, target_language: LanguageCode) -> str:
        ...

    def translate_multilingual(self, text: MultilingualText, source_language: LanguageCode, target_languages: tuple[LanguageCode, ...] = ("en", "fr", "de", "it"),) -> MultilingualText:
        ...

