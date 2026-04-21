# graphregistry/workflows/factories/fct_text.py
from __future__ import annotations
from graphregistry.domain.models.entities.mdl_text import (
    DEFAULT_LANGUAGE_CODES,
    GeneratedText,
    LanguageCode,
    LanguageCodeList,
    MultilingualGeneratedText,
)
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
    def create(
        self,
        *,
        source_language: LanguageCode,
        value: str,
        auto_translate_missing: bool = False,
        target_languages: LanguageCodeList = DEFAULT_LANGUAGE_CODES,
    ) -> MultilingualGeneratedText:

        # Create the MultilingualGeneratedText instance with the provided source language and value
        out = MultilingualGeneratedText()

        # Set the generated text for the source language
        setattr(out, source_language, GeneratedText(value=value))

        # If auto-translation of missing languages is requested, use the text operations to autofill the missing generated text for the target languages
        if auto_translate_missing:
            # Auto-translate missing languages using the text operations
            out = self.text_ops.autofill_missing_generated_text(
                text = out,
                source_language  = source_language,
                target_languages = target_languages,
            )

        # Return the MultilingualGeneratedText instance with the generated text
        return out
