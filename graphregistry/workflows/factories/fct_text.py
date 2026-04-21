# graphregistry/workflows/factories/fct_text.py
from __future__ import annotations
from graphregistry.domain.models.entities.mdl_text import (
    GeneratedText,
    LanguageCode,
    MultilingualGeneratedText,
)
from graphregistry.workflows.operations.entities.ops_text import GeneratedTextOperations

# Class definition
class MultilingualGeneratedTextFactory:
    def __init__(self, text_ops: GeneratedTextOperations) -> None:
        self.text_ops = text_ops

    def create(self, *, source_language: LanguageCode, value: str, auto_translate_missing: bool = False, target_languages: tuple[LanguageCode, ...] = ("en", "fr", "de", "it")) -> MultilingualGeneratedText:
        out = MultilingualGeneratedText()
        setattr(out, source_language, GeneratedText(value=value))
        if auto_translate_missing:
            out = self.text_ops.autofill_missing_generated_text(
                text=out,
                source_language=source_language,
                target_languages=target_languages,
            )
        return out
