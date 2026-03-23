from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field
from graphregistry.domain.models.mdl_base import NodeKey
from graphregistry.domain.models.mdl_text import MultilingualText, MultilingualGeneratedText, DescriptionSet

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

    @classmethod
    def from_json(cls, data: dict[str, Any], key: NodeKey) -> "PageProfile":
        instance = cls(key=key)
        instance.set_from_json(data)
        return instance

    def set_from_json(self, json_data: dict[str, Any]) -> None:
        languages = ("en", "fr", "de", "it")
        generated_attrs = (
            "is_auto_generated",
            "is_auto_corrected",
            "is_auto_translated",
            "translated_from",
            "value",
        )

        # Keep existing key unless explicitly provided
        if "key" in json_data:
            self.key = NodeKey.model_validate(json_data["key"])

        # Simple scalar field
        self.short_code = str(json_data.get("short_code", self.short_code) or "")

        # MySQL often returns 0/1 for booleans
        if "is_visible" in json_data:
            self.is_visible = bool(json_data["is_visible"])

        # ---- Simple multilingual text fields ----
        for lang in languages:
            numeric_id_key = f"numeric_id_{lang}"
            subtype_key = f"subtype_{lang}"
            external_key_key = f"external_key_{lang}"
            external_url_key = f"external_url_{lang}"

            if numeric_id_key in json_data:
                setattr(self.numeric_id, lang, str(json_data[numeric_id_key] or ""))

            if subtype_key in json_data:
                setattr(self.subtype, lang, str(json_data[subtype_key] or ""))

            if external_key_key in json_data:
                setattr(self.external_key, lang, str(json_data[external_key_key] or ""))

            if external_url_key in json_data:
                setattr(self.external_url, lang, str(json_data[external_url_key] or ""))

        # ---- Multilingual generated text: name ----
        for lang in languages:
            target = getattr(self.name, lang)
            for attr in generated_attrs:
                flat_key = f"name_{lang}_{attr}"
                if flat_key in json_data:
                    value = json_data[flat_key]
                    if attr.startswith("is_auto_"):
                        setattr(target, attr, bool(value))
                    else:
                        setattr(target, attr, str(value or ""))

        # ---- Descriptions: short / medium / long ----
        for size in ("short", "medium", "long"):
            size_obj = getattr(self.description, size)
            for lang in languages:
                target = getattr(size_obj, lang)
                for attr in generated_attrs:
                    flat_key = f"description_{size}_{lang}_{attr}"
                    if flat_key in json_data:
                        value = json_data[flat_key]
                        if attr.startswith("is_auto_"):
                            setattr(target, attr, bool(value))
                        else:
                            setattr(target, attr, str(value or ""))

    def to_json(self) -> dict[str, Any]:
        return self.model_dump()

    def to_simplified_dict(self) -> dict[str, Any]:

        flattened = {
            "institution_id" : self.key.institution_id,
            "object_type"    : self.key.object_type,
            "object_id"      : self.key.object_id,
            "short_code"     : self.short_code,
            "is_visible"     : self.is_visible,
        }

        # Flatten multilingual text fields
        for lang in ("en", "fr", "de", "it"):
            flattened[f"numeric_id_{lang}"] = getattr(self.numeric_id, lang)
            flattened[f"subtype_{lang}"] = getattr(self.subtype, lang)
            flattened[f"external_key_{lang}"] = getattr(self.external_key, lang)
            flattened[f"external_url_{lang}"] = getattr(self.external_url, lang)

            name_lang_obj = getattr(self.name, lang)
            for attr in ("is_auto_generated", "is_auto_corrected", "is_auto_translated", "translated_from", "value"):
                flattened[f"name_{lang}_{attr}"] = int(getattr(name_lang_obj, attr)) if isinstance(getattr(name_lang_obj, attr), bool) else getattr(name_lang_obj, attr)

            for size in ("short", "medium", "long"):
                desc_lang_obj = getattr(getattr(self.description, size), lang)
                for attr in ("is_auto_generated", "is_auto_corrected", "is_auto_translated", "translated_from", "value"):
                    flattened[f"description_{size}_{lang}_{attr}"] = int(getattr(desc_lang_obj, attr)) if isinstance(getattr(desc_lang_obj, attr), bool) else getattr(desc_lang_obj, attr)

        # Remove keys with None or empty string values
        flattened = {k: v for k, v in flattened.items() if v not in (None, "")}

        # Sort keys for consistent ordering
        flattened = dict(sorted(flattened.items()))

        # Remove object key
        flattened.pop("institution_id", None)
        flattened.pop("object_type", None)
        flattened.pop("object_id", None)

        return flattened