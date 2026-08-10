# graphregistry/domain/models/entities/mdl_text.py
from __future__ import annotations
from typing import TYPE_CHECKING, Iterator, Any
from pydantic import BaseModel, Field
from graphregistry.domain.types import LanguageCode, LanguageCodeList, DEFAULT_LANGUAGE_CODES


from rich.console import Console, ConsoleOptions, RenderResult

from rich.table import Table

from rich.panel import Panel

from rich.text import Text

# Check type if running in a type-checking context to avoid circular imports
if TYPE_CHECKING:
    from graphregistry.application.ports.gateways.prt_translation import TextTranslationGateway

# Model definition
class MultilingualText(BaseModel):
    """Model representing a multilingual text, with language codes as keys and text as values.
    The item_map field is a dictionary mapping language codes to their corresponding text values.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    item_map: dict[LanguageCode, str] = Field(default_factory=dict)

    #-----------------------------------#
    # Model constructors and validators #
    #-----------------------------------#
    def __init__(self, item_map: dict[LanguageCode, str] | None = None, /, **data) -> None:
        if item_map is not None and 'item_map' not in data:
            data['item_map'] = item_map
        super(MultilingualText, self).__init__(**data)

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, input_json: dict[str, str] | dict[str, dict[str, str]]) -> "MultilingualText":
        if 'item_map' in input_json and isinstance(input_json['item_map'], dict):
            return cls(item_map=input_json['item_map'])
        return cls(item_map={str(k): str(v) for k, v in input_json.items()})

    def to_json(self) -> dict[str, str]:
        return dict(self.item_map)

    #--------------------#
    # Operator overloads #
    #--------------------#

    # Operator: multilingual_text[language] to get the text for a specific language
    def __getitem__(self, language: LanguageCode) -> str:
        return self.get(language)

    # Operator: multilingual_text[language] = value to set the text for a specific language
    def __setitem__(self, language: LanguageCode, value: str) -> None:
        self.set(language, value)

    # Method: Get keys of the item_map directly from the object
    def keys(self) -> Iterator[LanguageCode]:
        return iter(self.item_map.keys())

    # Operator: Iterate directly over language codes
    def __iter__(self) -> Iterator[LanguageCode]: # type: ignore
        return iter(self.item_map)

    #----------------#
    # Access methods #
    #----------------#

    # Method: Get the text for a specific language
    def get(self, language: LanguageCode, default: str = "") -> str:
        return self.item_map.get(language, default)

    # Method: Get the text for a specific language, returning an empty string if not found
    def get_value(self, language: LanguageCode) -> str:
        return self.item_map.get(language, "")

    # Method: Set the text for a specific language
    def set(self, language: LanguageCode, value: str) -> None:
        self.item_map[language] = str(value)

# Model definition
class GeneratedText(BaseModel):
    """Model representing a generated text in a specific language,
    along with metadata about how it was generated or translated.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    is_auto_generated:  bool = False
    is_auto_corrected:  bool = False
    is_auto_translated: bool = False
    translated_from: LanguageCode | None = None
    value: str = ""

# Model definition
class MultilingualGeneratedText(BaseModel):
    """Model representing a multilingual text with rich metadata for each language,
    including whether it was auto-generated, auto-corrected, or auto-translated,
    and the source language if it was translated.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    item_map: dict[LanguageCode, GeneratedText] = Field(default_factory=dict)
    LANGUAGES: LanguageCodeList = DEFAULT_LANGUAGE_CODES

    #-----------------------------------#
    # Model constructors and validators #
    #-----------------------------------#
    def __init__(self, item_map: dict[LanguageCode, GeneratedText | dict[str, Any] | str] | None = None, /, **data: Any) -> None:

        # Extract dynamic language items from data (those that are not 'item_map' or class fields)
        dynamic_items: dict[LanguageCode, GeneratedText | dict[str, Any] | str] = {}
        model_fields = getattr(self.__class__, "model_fields", {})
        for key in list(data.keys()):
            if key != 'item_map' and key not in model_fields:
                dynamic_items[key] = data.pop(key)

        # Merge all items into a single dictionary
        merged: dict[LanguageCode, GeneratedText] = {}

        # First, process the item_map if it exists
        if item_map is not None:
            for lang, raw_value in item_map.items():
                merged[str(lang)] = self._coerce_generated_text(raw_value)

        # Then, process any explicit item_map provided in data (this will override the initial item_map if both are provided)
        explicit_item_map = data.pop('item_map', None)
        if isinstance(explicit_item_map, dict):
            for lang, raw_value in explicit_item_map.items():
                merged[str(lang)] = self._coerce_generated_text(raw_value)
        elif explicit_item_map is not None:
            data['item_map'] = explicit_item_map

        # Finally, process any dynamic items provided directly in data (these will override both previous sources)
        for lang, raw_value in dynamic_items.items():
            merged[str(lang)] = self._coerce_generated_text(raw_value)

        # If we have any merged items, set them in the data for the BaseModel constructor
        if merged:
            data['item_map'] = merged

        # Call the BaseModel constructor with the processed data
        super(MultilingualGeneratedText, self).__init__(**data)

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, json_data: dict) -> "MultilingualGeneratedText":
        if 'item_map' in json_data and isinstance(json_data['item_map'], dict):
            return cls(item_map=json_data['item_map'])
        return cls(item_map=json_data)

    def to_json(self) -> dict:
        return {
            lang: self.get(lang).model_dump(mode="json")
            for lang in self._ordered_languages()
        }

    #------------------------------#
    # Helper and auxiliary methods #
    #------------------------------#

    # Method: Coerce different input types into a GeneratedText instance
    @staticmethod
    def _coerce_generated_text(value: GeneratedText | dict[str, Any] | str) -> GeneratedText:
        if isinstance(value, GeneratedText):
            return value
        if isinstance(value, str):
            return GeneratedText(value=value)
        if isinstance(value, dict):
            return GeneratedText.model_validate(value)
        raise TypeError(f"Unsupported generated text value type: {type(value)!r}")

    # Method: Return an ordered tuple of languages, prioritizing those in LANGUAGES
    # but including any additional ones from item_map
    def _ordered_languages(self) -> tuple[LanguageCode, ...]:
        lang_list: list[LanguageCode] = list(self.LANGUAGES)
        for lang in self.item_map.keys():
            if lang not in lang_list:
                lang_list.append(lang)
        return tuple(lang_list)

    #----------------#
    # Access methods #
    #----------------#

    # Method: Get the GeneratedText for a specific language, returning an empty GeneratedText if not found
    def get(self, language: LanguageCode) -> GeneratedText:
        return self.item_map.get(language, GeneratedText())

    # Method: Get the text value for a specific language, returning an empty string if not found
    def get_value(self, language: LanguageCode) -> str:
        return self.get(language).value

    # Method: Set the text value for a specific language, with optional metadata about how it was generated or translated
    def set(self, language: LanguageCode, value: str, *,
        is_auto_generated: bool | None = None,
        is_auto_corrected: bool | None = None,
        is_auto_translated: bool | None = None,
        translated_from: LanguageCode | None = None,
    ) -> None:
        current = self.get(language)
        self.item_map[language] = current.model_copy(
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
        )

    #-----------------------------------------#
    # Factory methods and translation helpers #
    #-----------------------------------------#

    # Method: Factory method to create an instance from source data,
    # with optional auto-translation of missing languages
    @classmethod
    def from_source(cls, *,
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
        # First, create the object with the provided data (which may include some translations)
        obj = cls(**data)

        # Then, if auto-translation is enabled, fill in any missing translations using the provided gateway
        if auto_translate_missing:

            # If auto-translation is requested but no gateway is provided, we cannot proceed, so raise an error
            if translation_gateway is None:
                raise ValueError("translation_gateway is required when auto_translate_missing=True")

            # Fill missing translations in-place on the created object
            obj.fill_missing_translations(
                translation_gateway=translation_gateway,
                source_language=source_language,
                overwrite_existing=overwrite_existing,
                force=force,
                no_cache=no_cache,
                skip_segmentation=skip_segmentation,
                clean=clean,
            )

        # Finally, return the created (and possibly auto-translated) object
        return obj

    # Method: Fill in missing translations in-place using the provided translation gateway
    def fill_missing_translations(self, *,
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
        # If no source language is provided, attempt to detect it from the existing translations.
        if source_language is None:
            source_language = "en"  # Default to English if no source language is specified

        # If we still don't have a source language after detection, we cannot proceed with translation, so return early.
        if source_language is None:
            return

        # Get the source value for the specified source language.
        # If it's empty or only whitespace, we cannot translate, so return early.
        source_value = self.get_value(source_language).strip()
        if not source_value:
            return

        # Use the translation gateway to translate the source value into the target languages.
        translated = translation_gateway.translate_multilingual(
            text=MultilingualText(item_map={source_language: source_value}),
            source_language=source_language,
            target_languages=self.LANGUAGES,
        )

        # Iterate over the target languages and update self with the translated values,
        # respecting the overwrite_existing flag.
        for lang in self.LANGUAGES:

            # Skip the source language since we don't need to translate it.
            if lang == source_language:
                continue

            # If overwrite_existing is False, check if we already have a non-empty value
            # for this language. If so, skip it.
            current = self.get(lang)

            # If the current value is non-empty and we're not allowed to overwrite
            # existing translations, skip this language.
            if current.value.strip() and not overwrite_existing:
                continue

            # Get the new translated value for this language. If it's empty or only whitespace, skip it.
            new_value = translated.get(lang).strip()
            if not new_value:
                continue

            # Update self with the new translated value, marking it as auto-translated and
            # recording the source language.
            self.set(lang, new_value, is_auto_translated=True, translated_from=source_language)

    #--------------------#
    # Operator overloads #
    #--------------------#

    # Operator: multilingual_generated_text[language] to get the GeneratedText for a specific language
    def __str__(self) -> str:
        parts = []
        for lang in self._ordered_languages():
            value = self.get_value(lang).strip()
            if value:
                parts.append(f"{lang}={value!r}")
        return f"MultilingualGeneratedText({', '.join(parts)})"

    # Operator: multilingual_generated_text[language] = value to set the GeneratedText for a specific language
    def __getitem__(self, language: LanguageCode) -> GeneratedText:
        return self.get(language)

    # Operator: multilingual_generated_text[language] = value to set the GeneratedText for a specific language
    def __setitem__(self, language: LanguageCode, value: GeneratedText | dict[str, Any] | str) -> None:
        self.item_map[language] = self._coerce_generated_text(value)

    # Operator: Iterate directly over language codes
    def __iter__(self) -> Iterator[LanguageCode]: # type: ignore
        return iter(self._ordered_languages())

    # Operator: Get keys of the item_map directly from the object
    def keys(self) -> Iterator[LanguageCode]:
        return iter(self._ordered_languages())

    # Operator: getattr(obj, "en") style access to get the GeneratedText for a specific language
    def __getattr__(self, name: str) -> GeneratedText:
        # Backward compatibility for getattr(obj, "en") style access.
        if name.startswith("_"):
            raise AttributeError(name)
        item_map = self.__dict__.get('item_map')
        if isinstance(item_map, dict):
            return item_map.get(name, GeneratedText())
        raise AttributeError(name)

    # Operator: setattr(obj, "en", value) style access to set the GeneratedText for a specific language
    def __setattr__(self, name: str, value: Any) -> None:
        # Backward compatibility for setattr(obj, "en", GeneratedText(...)).
        model_fields = getattr(self.__class__, "model_fields", {})
        if name in model_fields or name.startswith("_"):
            super(MultilingualGeneratedText, self).__setattr__(name, value)
            return

        item_map = self.__dict__.get('item_map')
        if isinstance(item_map, dict):
            item_map[name] = self._coerce_generated_text(value)
            return

        super(MultilingualGeneratedText, self).__setattr__(name, value)

# Model definition
class DescriptionSet(BaseModel):
    """Model representing a set of descriptions in multiple languages, categorized by length (short, medium, long).
    Each description is represented as a MultilingualGeneratedText, allowing for rich metadata and auto-translation capabilities.
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    short:  MultilingualGeneratedText = Field(default_factory=MultilingualGeneratedText)
    medium: MultilingualGeneratedText = Field(default_factory=MultilingualGeneratedText)
    long:   MultilingualGeneratedText = Field(default_factory=MultilingualGeneratedText)

#=========================================#
# Example usage and testing from terminal #
#=========================================#
if __name__ == "__main__":

    from graphregistry.adapters.gateways.graphai import GraphAITextTranslationGateway

    print("\n--- INIT ---")
    gtw = GraphAITextTranslationGateway(debug=True)

    # ------------------------------------------------------------------
    # 1. Factory creation WITHOUT translation
    # ------------------------------------------------------------------
    print("\n--- from_source (no translation) ---")
    t = MultilingualGeneratedText.from_source(
        fr=GeneratedText(value="Bonjour tout le monde"),
        auto_translate_missing=False,
    )
    print(t)

    # ------------------------------------------------------------------
    # 2. Factory creation WITH translation
    # ------------------------------------------------------------------
    print("\n--- from_source (auto translation) ---")
    t2 = MultilingualGeneratedText.from_source(
        fr=GeneratedText(value="Bonjour tout le monde"),
        translation_gateway=gtw,
        auto_translate_missing=True,
        source_language="fr",
    )
    print(t2)

    # ------------------------------------------------------------------
    # 3. fill_missing_translations (in-place)
    # ------------------------------------------------------------------
    print("\n--- fill_missing_translations ---")
    t3 = MultilingualGeneratedText(
        fr=GeneratedText(value="Je suis un test")
    )
    t3.fill_missing_translations(
        translation_gateway=gtw,
        source_language="fr",
    )
    print(t3)
