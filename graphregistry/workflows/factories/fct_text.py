# graphregistry/workflows/factories/fct_text.py
from __future__ import annotations
from graphregistry.domain.models.entities.mdl_text import GeneratedText, LanguageCode, MultilingualGeneratedText
from graphregistry.workflows.operations.entities.ops_text import GeneratedTextOperations

# Factory definition
class MultilingualGeneratedTextFactory:
    """Factory for creating MultilingualGeneratedText instances, with
    optional auto-translation of missing languages. The factory uses a
    GeneratedTextOperations instance to perform auto-translation when requested.
    """
    # Class constructor
    def __init__(self, text_ops: GeneratedTextOperations) -> None:
        self.text_ops = text_ops

    # Method: Create a MultilingualGeneratedText instance with optional auto-translation of missing languages
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
