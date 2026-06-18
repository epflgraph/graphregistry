# tests/unit_tests/domain/test_mdl_text.py
"""Unit tests for multilingual text value objects."""
from __future__ import annotations

from graphregistry.domain.models.entities.mdl_text import (
    DescriptionSet,
    GeneratedText,
    MultilingualGeneratedText,
    MultilingualText,
)


class TestMultilingualText:
    def test_set_and_get(self) -> None:
        text = MultilingualText()
        text.set("en", "Hello")
        text.set("fr", "Bonjour")
        assert text.get("en") == "Hello"
        assert text.get("fr") == "Bonjour"
        assert text.get("de") == ""

    def test_getitem_and_setitem(self) -> None:
        text = MultilingualText()
        text["en"] = "Hello"
        assert text["en"] == "Hello"

    def test_from_json_flat(self) -> None:
        text = MultilingualText.from_json({"en": "Hello", "fr": "Bonjour"})
        assert text.to_json() == {"en": "Hello", "fr": "Bonjour"}

    def test_from_json_with_item_map(self) -> None:
        text = MultilingualText.from_json({"item_map": {"en": "Hello"}})
        assert text.get("en") == "Hello"

    def test_init_with_positional_dict(self) -> None:
        text = MultilingualText({"en": "Hello"})
        assert text.get("en") == "Hello"


class TestMultilingualGeneratedText:
    def test_set_value(self) -> None:
        text = MultilingualGeneratedText()
        text.set("en", "Hello", is_auto_generated=True)
        assert text.get_value("en") == "Hello"
        assert text.get("en").is_auto_generated is True

    def test_setitem_with_string(self) -> None:
        text = MultilingualGeneratedText()
        text["en"] = "Hello"
        assert text.get_value("en") == "Hello"

    def test_setitem_with_generated_text(self) -> None:
        text = MultilingualGeneratedText()
        text["en"] = GeneratedText(value="Hello", is_auto_translated=True)
        assert text.get("en").is_auto_translated is True

    def test_from_source_no_translation(self) -> None:
        text = MultilingualGeneratedText.from_source(en=GeneratedText(value="Hello"))
        assert text.get_value("en") == "Hello"

    def test_getattr_access(self) -> None:
        text = MultilingualGeneratedText()
        text["en"] = "Hello"
        assert text.en.value == "Hello"

    def test_to_json(self) -> None:
        text = MultilingualGeneratedText()
        text.set("en", "Hello")
        assert "en" in text.to_json()
        assert text.to_json()["en"]["value"] == "Hello"


class TestDescriptionSet:
    def test_default_description_set(self) -> None:
        desc = DescriptionSet()
        assert desc.short.get_value("en") == ""
        desc.short.set("en", "Short summary")
        assert desc.short.get_value("en") == "Short summary"
