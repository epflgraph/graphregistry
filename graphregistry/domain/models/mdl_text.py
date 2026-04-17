# graphregistry/domain/models/mdl_text.py
from __future__ import annotations
from typing import TYPE_CHECKING, Literal, TypeAlias
from pydantic import BaseModel, Field

# Check type if running in a type-checking context to avoid circular imports
if TYPE_CHECKING:
    from graphregistry.domain.interfaces.gateways.gtw_translation import TextTranslationGateway

# Type alias for language codes
LanguageCode: TypeAlias = Literal["en", "fr", "de", "it"]

# Model definition
class MultilingualText(BaseModel):
    en: str = ""
    fr: str = ""
    de: str = ""
    it: str = ""

# Model definition
class GeneratedText(BaseModel):
    is_auto_generated:  bool = False
    is_auto_corrected:  bool = False
    is_auto_translated: bool = False
    translated_from: LanguageCode | None = None
    value: str = ""

# Model definition
class MultilingualGeneratedText(BaseModel):
    en: GeneratedText = Field(default_factory=GeneratedText)
    fr: GeneratedText = Field(default_factory=GeneratedText)
    de: GeneratedText = Field(default_factory=GeneratedText)
    it: GeneratedText = Field(default_factory=GeneratedText)

    # Class method: Fill missing translations using a provided translation gateway
    def fill_missing_translations(
        self, *, translation_gateway: "TextTranslationGateway", source_language: LanguageCode | None = None, overwrite_existing: bool = False) -> None:
        """
        Mutates self in-place using only translation.
        """

        # Auto-detect source language, while prioritizing English if available
        if source_language is None:
            for lang in ("en", "fr", "de", "it"):
                if getattr(self, lang).value.strip():
                    source_language = lang
                    break

        # If no source language could be detected, there's nothing we can do
        if source_language is None:
            return

        # If the source language value is empty, there's also nothing we can do
        source_value = getattr(self, source_language).value.strip()
        if not source_value:
            return

        # Build simple multilingual payload
        seed = MultilingualText()
        setattr(seed, source_language, source_value)

        # Execute translation by calling the gateway
        translated = translation_gateway.translate_multilingual(text=seed, source_language=source_language)

        # Apply results to self, with optional overwrite protection and metadata
        for lang in ("en", "fr"):

            # Skip source language and any languages that already have a value (if overwrite is disabled)
            if lang == source_language:
                continue

            # Get current value for this language
            current = getattr(self, lang)

            # If there's already a non-empty value and overwrite is disabled, skip applying translation to this language
            if current.value.strip() and not overwrite_existing:
                continue

            # Get translated value for this language, and if it's empty, skip applying it
            new_value = getattr(translated, lang, "").strip()
            if not new_value:
                continue

            # Apply translated value and metadata to self
            setattr(self, lang, current.model_copy(
                update={
                    "value": new_value,
                    "is_auto_translated": True,
                    "translated_from": source_language,
                }
            ))

# Model definition
class DescriptionSet(BaseModel):
    short:  MultilingualGeneratedText = Field(default_factory=MultilingualGeneratedText)
    medium: MultilingualGeneratedText = Field(default_factory=MultilingualGeneratedText)
    long:   MultilingualGeneratedText = Field(default_factory=MultilingualGeneratedText)
