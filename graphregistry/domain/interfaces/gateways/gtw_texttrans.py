from __future__ import annotations

from typing import Protocol

from graphregistry.domain.models.mdl_text import MultilingualText


class TextTranslationGateway(Protocol):
    def translate_text(self, text: str, source_language: str, target_language: str) -> str:
        ...

    def translate_multilingual(
        self,
        text: MultilingualText,
        source_language: str,
        target_languages: tuple[str, ...] = ("en", "fr", "de", "it"),
    ) -> MultilingualText:
        ...
