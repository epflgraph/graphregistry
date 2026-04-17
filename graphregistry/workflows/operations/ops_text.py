# graphregistry/workflows/operations/ops_text.py
# graphregistry/workflows/operations/ops_text.py
from __future__ import annotations
from graphregistry.domain.interfaces.gateways.gtw_textgen import TextGenerationGateway
from graphregistry.domain.interfaces.gateways.gtw_translation import TextTranslationGateway
from graphregistry.domain.models.mdl_text import (
    GeneratedText,
    LanguageCode,
    MultilingualGeneratedText,
    MultilingualText,
)

ALL_LANGUAGES: tuple[LanguageCode, ...] = ("en", "fr", "de", "it")


class GeneratedTextOperations:
    """
    Use-case layer for generated/translatable text.
    """

    def __init__(
        self,
        translation_gateway: TextTranslationGateway,
        generation_gateway: TextGenerationGateway,
    ) -> None:
        self.translation_gateway = translation_gateway
        self.generation_gateway = generation_gateway

    def translate_multilingual(
        self,
        text: MultilingualText,
        source_language: LanguageCode,
        target_languages: tuple[LanguageCode, ...] = ALL_LANGUAGES,
    ) -> MultilingualText:
        return self.translation_gateway.translate_multilingual(
            text=text,
            source_language=source_language,
            target_languages=target_languages,
        )

    def generate_text(self, prompt: str, language: LanguageCode = "en") -> GeneratedText:
        return self.generation_gateway.generate_text(prompt=prompt, language=language)

    def autofill_missing_generated_text(
        self,
        text: MultilingualGeneratedText,
        source_language: LanguageCode,
        target_languages: tuple[LanguageCode, ...] = ALL_LANGUAGES,
        overwrite_existing: bool = False,
    ) -> MultilingualGeneratedText:
        """
        Fill missing language values by translating from `source_language`.

        Rules:
        - never translate if source is empty
        - by default, do not overwrite existing target values
        - preserve existing metadata on already-filled fields
        """
        source_obj = getattr(text, source_language)
        source_value = source_obj.value.strip()

        out = text.model_copy(deep=True)

        if not source_value:
            return out

        # Build a simple multilingual payload for the translation gateway
        seed = MultilingualText()
        setattr(seed, source_language, source_value)

        translated = self.translation_gateway.translate_multilingual(
            text=seed,
            source_language=source_language,
            target_languages=target_languages,
        )

        for lang in target_languages:
            current = getattr(out, lang)

            if lang == source_language:
                continue

            if current.value.strip() and not overwrite_existing:
                continue

            translated_value = getattr(translated, lang, "").strip()
            if not translated_value:
                continue

            setattr(
                out,
                lang,
                current.model_copy(
                    update={
                        "value": translated_value,
                        "is_auto_translated": True,
                        "translated_from": source_language,
                    }
                ),
            )

        return out
