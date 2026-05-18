from __future__ import annotations

import pytest

from graphregistry.domain.models.entities.mdl_text import DescriptionSet, GeneratedText, MultilingualGeneratedText, MultilingualText


def test_multilingual_text_defaults_empty_strings() -> None:
    text = MultilingualText()

    assert text.en == ""
    assert text.fr == ""
    assert text.de == ""
    assert text.it == ""


def test_generated_text_defaults_and_language_validation() -> None:
    generated = GeneratedText(value="hello")

    assert generated.is_auto_generated is False
    assert generated.is_auto_corrected is False
    assert generated.is_auto_translated is False
    assert generated.translated_from is None

    with pytest.raises(Exception):
        GeneratedText(translated_from="es", value="hola")  # type: ignore[arg-type]


def test_multilingual_generated_text_has_independent_language_objects() -> None:
    text = MultilingualGeneratedText()
    text.en.value = "English"

    assert text.en.value == "English"
    assert text.fr.value == ""


def test_description_set_builds_all_sizes() -> None:
    desc = DescriptionSet()

    assert isinstance(desc.short, MultilingualGeneratedText)
    assert isinstance(desc.medium, MultilingualGeneratedText)
    assert isinstance(desc.long, MultilingualGeneratedText)
