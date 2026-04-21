# graphregistry/domain/models/mdl_pageprofile.py
from __future__ import annotations
from typing import Iterator
from pydantic import BaseModel, Field, model_validator
from graphregistry.domain.models.entities.mdl_base import NodeKey
from graphregistry.domain.models.entities.mdl_text import (
    DescriptionSet,
    GeneratedText,
    MultilingualGeneratedText,
    MultilingualText,
    LanguageCode,
)

# Model definition
class PageProfile(BaseModel):
    key          : NodeKey
    numeric_id   : MultilingualText = Field(default_factory=MultilingualText)
    short_code   : str = ""
    subtype      : MultilingualText = Field(default_factory=MultilingualText)
    name         : MultilingualGeneratedText = Field(default_factory=MultilingualGeneratedText)
    description  : DescriptionSet   = Field(default_factory=DescriptionSet)
    external_key : MultilingualText = Field(default_factory=MultilingualText)
    external_url : MultilingualText = Field(default_factory=MultilingualText)
    is_visible   : bool = True

    @model_validator(mode="after")
    def validate_key_type(self) -> "PageProfile":
        if not isinstance(self.key, NodeKey):
            raise TypeError("key must be a NodeKey")
        return self

    @classmethod
    def from_json(cls, json_data: dict) -> "PageProfile":
        return cls.model_validate(json_data)

    def to_json(self) -> dict:
        return self.model_dump(mode="json")

    def set_visibility(self, is_visible: bool) -> None:
        self.is_visible = bool(is_visible)

    def get_numeric_id(self, language: LanguageCode) -> str:
        return getattr(self.numeric_id, language)

    def set_numeric_id(self, language: LanguageCode, value: str) -> None:
        setattr(self.numeric_id, language, str(value))

    def get_subtype(self, language: LanguageCode) -> str:
        return getattr(self.subtype, language)

    def set_subtype(self, language: LanguageCode, value: str) -> None:
        setattr(self.subtype, language, str(value))

    def get_external_key(self, language: LanguageCode) -> str:
        return getattr(self.external_key, language)

    def set_external_key(self, language: LanguageCode, value: str) -> None:
        setattr(self.external_key, language, str(value))

    def get_external_url(self, language: LanguageCode) -> str:
        return getattr(self.external_url, language)

    def set_external_url(self, language: LanguageCode, value: str) -> None:
        setattr(self.external_url, language, str(value))

    def get_name(self, language: LanguageCode) -> GeneratedText:
        return getattr(self.name, language)

    def set_name(
        self,
        language: LanguageCode,
        value: str,
        *,
        is_auto_generated: bool | None = None,
        is_auto_corrected: bool | None = None,
        is_auto_translated: bool | None = None,
        translated_from: LanguageCode | None = None,
    ) -> None:
        current = self.get_name(language)
        self.name = self.name.model_copy(
            update={
                language: current.model_copy(
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
            }
        )

    def get_description(self, size: str, language: LanguageCode) -> GeneratedText:
        return getattr(getattr(self.description, size), language)

    def set_description(
        self,
        size: str,
        language: LanguageCode,
        value: str,
        *,
        is_auto_generated: bool | None = None,
        is_auto_corrected: bool | None = None,
        is_auto_translated: bool | None = None,
        translated_from: LanguageCode | None = None,
    ) -> None:
        if size not in ("short", "medium", "long"):
            raise ValueError("size must be one of: short, medium, long")

        current_size = getattr(self.description, size)
        current_text = getattr(current_size, language)

        updated_text = current_text.model_copy(
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

        updated_size = current_size.model_copy(update={language: updated_text})
        self.description = self.description.model_copy(update={size: updated_size})

    def has_any_name(self) -> bool:
        return any(
            getattr(self.name, lang).value.strip()
            for lang in ("en", "fr", "de", "it")
        )

    def has_any_description(self, size: str | None = None) -> bool:
        sizes = (size,) if size is not None else ("short", "medium", "long")
        for current_size in sizes:
            if current_size not in ("short", "medium", "long"):
                raise ValueError("size must be one of: short, medium, long")
            if any(
                getattr(getattr(self.description, current_size), lang).value.strip()
                for lang in ("en", "fr", "de", "it")
            ):
                return True
        return False

    def preferred_name(self, preferred_languages: tuple[LanguageCode, ...] = ("en", "fr", "de", "it")) -> str:
        for lang in preferred_languages:
            value = self.get_name(lang).value.strip()
            if value:
                return value
        return ""

    def preferred_description(
        self,
        size: str = "short",
        preferred_languages: tuple[LanguageCode, ...] = ("en", "fr", "de", "it"),
    ) -> str:
        if size not in ("short", "medium", "long"):
            raise ValueError("size must be one of: short, medium, long")

        for lang in preferred_languages:
            value = self.get_description(size, lang).value.strip()
            if value:
                return value
        return ""

    def iter_languages(self) -> Iterator[LanguageCode]:
        return iter(("en", "fr", "de", "it"))
