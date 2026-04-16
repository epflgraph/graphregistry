# graphregistry/workflows/operations/ops_text.py
from __future__ import annotations
from graphregistry.domain.interfaces.gateways.gtw_textgen import TextGenerationGateway
from graphregistry.domain.interfaces.gateways.gtw_texttrans import TextTranslationGateway
from graphregistry.domain.models.mdl_text import (
    GeneratedText,
    LanguageCode,
    MultilingualGeneratedText,
    MultilingualText,
)

# Class definition
class GeneratedTextOperations:
    """
    Use-case layer for generated/translatable text.
    """

    def __init__(self, translation_gateway: TextTranslationGateway, generation_gateway: TextGenerationGateway) -> None:
        self.translation_gateway = translation_gateway
        self.generation_gateway = generation_gateway

    def translate_multilingual(self, text: MultilingualText, source_language: LanguageCode, target_languages: tuple[LanguageCode, ...] = ("en", "fr", "de", "it")) -> MultilingualText:
        return self.translation_gateway.translate_multilingual(
            text=text,
            source_language=source_language,
            target_languages=target_languages,
        )

    def generate_text(self, prompt: str, language: LanguageCode = "en") -> GeneratedText:
        return self.generation_gateway.generate_text(prompt=prompt, language=language)

    def generate_and_translate(self, prompt: str, source_language: LanguageCode = "en", target_languages: tuple[LanguageCode, ...] = ("en", "fr", "de", "it")) -> MultilingualGeneratedText:
        base = self.generate_text(prompt=prompt, language=source_language)

        seed = MultilingualText()
        setattr(seed, source_language, base.value)

        translated = self.translate_multilingual(
            text=seed,
            source_language=source_language,
            target_languages=target_languages,
        )

        out = MultilingualGeneratedText()
        for lang in target_languages:
            value = getattr(translated, lang, None)
            setattr(
                out,
                lang,
                GeneratedText(
                    is_auto_generated=(lang == source_language),
                    is_auto_translated=(lang != source_language),
                    translated_from=(source_language if lang != source_language else None),
                    value=(value or ""),
                ),
            )
        return out
