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

    LANGUAGES: tuple[LanguageCode, ...] = ("en", "fr", "de", "it")

    @classmethod
    def from_source(
        cls,
        *,
        translation_gateway: "TextTranslationGateway | None" = None,
        auto_translate_missing: bool = False,
        source_language: LanguageCode | None = None,
        overwrite_existing: bool = False,
        force: bool = False,
        no_cache: bool = False,
        skip_segmentation: bool = False,
        clean: bool = False,
        **data,
    ) -> "MultilingualGeneratedText":
        """
        Factory method:
        - creates the object
        - optionally fills missing translations
        """
        obj = cls(**data)

        if auto_translate_missing:
            if translation_gateway is None:
                raise ValueError("translation_gateway is required when auto_translate_missing=True")

            obj.fill_missing_translations(
                translation_gateway=translation_gateway,
                source_language=source_language,
                overwrite_existing=overwrite_existing,
                force=force,
                no_cache=no_cache,
                skip_segmentation=skip_segmentation,
                clean=clean,
            )

        return obj

    def fill_missing_translations(
        self,
        *,
        translation_gateway: "TextTranslationGateway",
        source_language: LanguageCode | None = None,
        overwrite_existing: bool = False,
        force: bool = False,
        no_cache: bool = False,
        skip_segmentation: bool = False,
        clean: bool = False,
    ) -> None:
        """
        Mutates self in-place using only translation.
        """
        if source_language is None:
            source_language = self.detect_source_language()

        if source_language is None:
            return

        source_value = self.get_value(source_language).strip()
        if not source_value:
            return

        translated = translation_gateway.translate_multilingual(
            text=MultilingualText(**{source_language: source_value}),
            source_language=source_language,
            target_languages=self.LANGUAGES,
        )

        for lang in self.LANGUAGES:
            if lang == source_language:
                continue

            current = self.get(lang)

            if current.value.strip() and not overwrite_existing:
                continue

            new_value = getattr(translated, lang, "").strip()
            if not new_value:
                continue

            self.set(
                lang,
                new_value,
                is_auto_translated=True,
                translated_from=source_language,
            )

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    def get(self, language: LanguageCode) -> GeneratedText:
        return getattr(self, language)

    def get_value(self, language: LanguageCode) -> str:
        return self.get(language).value

    def set(
        self,
        language: LanguageCode,
        value: str,
        *,
        is_auto_generated: bool | None = None,
        is_auto_corrected: bool | None = None,
        is_auto_translated: bool | None = None,
        translated_from: LanguageCode | None = None,
    ) -> None:
        current = self.get(language)
        setattr(
            self,
            language,
            current.model_copy(
                update={
                    "value": str(value),
                    **(
                        {"is_auto_generated": bool(is_auto_generated)}
                        if is_auto_generated is not None else {}
                    ),
                    **(
                        {"is_auto_corrected": bool(is_auto_corrected)}
                        if is_auto_corrected is not None else {}
                    ),
                    **(
                        {"is_auto_translated": bool(is_auto_translated)}
                        if is_auto_translated is not None else {}
                    ),
                    **(
                        {"translated_from": translated_from}
                        if translated_from is not None else {}
                    ),
                }
            ),
        )

    def has_value(self, language: LanguageCode) -> bool:
        return bool(self.get_value(language).strip())

    def preferred_source_language(self) -> LanguageCode | None:
        """
        Return the first language that contains text.
        Priority order is English first, then French, German, Italian.
        """
        for lang in self.LANGUAGES:
            if self.has_value(lang):
                return lang
        return None

    def preferred_value(
        self,
        preferred_languages: tuple[LanguageCode, ...] | None = None,
    ) -> str:
        for lang in preferred_languages or self.LANGUAGES:
            value = self.get_value(lang).strip()
            if value:
                return value
        return ""

    # ------------------------------------------------------------------
    # Iteration helpers
    # ------------------------------------------------------------------

    def iter_languages(self) -> Iterator[LanguageCode]:
        return iter(self.LANGUAGES)

    def iter_generated_texts(self) -> Iterator[tuple[LanguageCode, GeneratedText]]:
        for lang in self.LANGUAGES:
            yield lang, self.get(lang)

    def iter_values(self) -> Iterator[tuple[LanguageCode, str]]:
        for lang in self.LANGUAGES:
            yield lang, self.get_value(lang)

    def non_empty_languages(self) -> list[LanguageCode]:
        return [lang for lang in self.LANGUAGES if self.has_value(lang)]

    # ------------------------------------------------------------------
    # Serialization helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_json(cls, json_data: dict) -> "MultilingualGeneratedText":
        return cls.model_validate(json_data)

    def to_json(self) -> dict:
        return self.model_dump(mode="json")

    def to_simple_dict(self, *, include_empty: bool = True) -> dict[str, str]:
        out: dict[str, str] = {}
        for lang in self.LANGUAGES:
            value = self.get_value(lang)
            if include_empty or value.strip():
                out[lang] = value
        return out

    def to_metadata_dict(self, *, include_empty: bool = True) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for lang in self.LANGUAGES:
            item = self.get(lang)
            if include_empty or item.value.strip():
                out[lang] = item.model_dump(mode="json")
        return out

    def copy_deep(self) -> "MultilingualGeneratedText":
        return self.model_copy(deep=True)

    # ------------------------------------------------------------------
    # Rich / string display helpers
    # ------------------------------------------------------------------

    def print_json(self, *, include_empty: bool = True, simple: bool = False) -> None:
        from rich import print_json

        data = self.to_simple_dict(include_empty=include_empty) if simple else self.to_metadata_dict(include_empty=include_empty)
        print_json(data=data)

    def __str__(self) -> str:
        parts = []
        for lang in self.LANGUAGES:
            value = self.get_value(lang).strip()
            if value:
                parts.append(f"{lang}={value!r}")
        return f"MultilingualGeneratedText({', '.join(parts)})"

    def __bool__(self) -> bool:
        return any(self.has_value(lang) for lang in self.LANGUAGES)

    def __getitem__(self, language: LanguageCode) -> GeneratedText:
        return self.get(language)

# Model definition
class DescriptionSet(BaseModel):
    short:  MultilingualGeneratedText = Field(default_factory=MultilingualGeneratedText)
    medium: MultilingualGeneratedText = Field(default_factory=MultilingualGeneratedText)
    long:   MultilingualGeneratedText = Field(default_factory=MultilingualGeneratedText)
