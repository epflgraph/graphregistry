# tests/unit_tests/domain/models/test_mdl_pageprofile.py
from __future__ import annotations

import pytest

from graphregistry.domain.models.entities.mdl_base import NodeKey
from graphregistry.domain.models.entities.mdl_pageprofile import PageProfile


@pytest.fixture
def node_key() -> NodeKey:
    return NodeKey(
        institution_id="EPFL",
        object_type="Course",
        object_id="CS-101",
    )


@pytest.fixture
def profile(node_key: NodeKey) -> PageProfile:
    return PageProfile(key=node_key)


def test_validate_key_type(node_key: NodeKey) -> None:
    profile = PageProfile(key=node_key)
    assert profile.key == node_key


def test_from_json(node_key: NodeKey) -> None:
    data = {
        "key": node_key.model_dump(mode="json"),
        "short_code": "CS-101",
        "is_visible": False,
        "numeric_id": {"en": "101", "fr": "101", "de": "", "it": ""},
        "subtype": {"en": "Course", "fr": "Cours", "de": "", "it": ""},
        "external_key": {"en": "course-cs-101", "fr": "", "de": "", "it": ""},
        "external_url": {"en": "https://example.org/cs101", "fr": "", "de": "", "it": ""},
        "name": {
            "en": {
                "is_auto_generated": True,
                "is_auto_corrected": False,
                "is_auto_translated": False,
                "translated_from": None,
                "value": "Intro to CS",
            },
            "fr": {
                "is_auto_generated": False,
                "is_auto_corrected": True,
                "is_auto_translated": True,
                "translated_from": "en",
                "value": "Introduction à l'informatique",
            },
            "de": {
                "is_auto_generated": False,
                "is_auto_corrected": False,
                "is_auto_translated": False,
                "translated_from": None,
                "value": "",
            },
            "it": {
                "is_auto_generated": False,
                "is_auto_corrected": False,
                "is_auto_translated": False,
                "translated_from": None,
                "value": "",
            },
        },
        "description": {
            "short": {
                "en": {
                    "is_auto_generated": True,
                    "is_auto_corrected": False,
                    "is_auto_translated": False,
                    "translated_from": None,
                    "value": "Short description",
                },
                "fr": {
                    "is_auto_generated": False,
                    "is_auto_corrected": False,
                    "is_auto_translated": True,
                    "translated_from": "en",
                    "value": "Description courte",
                },
                "de": {
                    "is_auto_generated": False,
                    "is_auto_corrected": False,
                    "is_auto_translated": False,
                    "translated_from": None,
                    "value": "",
                },
                "it": {
                    "is_auto_generated": False,
                    "is_auto_corrected": False,
                    "is_auto_translated": False,
                    "translated_from": None,
                    "value": "",
                },
            },
            "medium": {
                "en": {
                    "is_auto_generated": False,
                    "is_auto_corrected": False,
                    "is_auto_translated": False,
                    "translated_from": None,
                    "value": "Medium description",
                },
                "fr": {
                    "is_auto_generated": False,
                    "is_auto_corrected": False,
                    "is_auto_translated": False,
                    "translated_from": None,
                    "value": "",
                },
                "de": {
                    "is_auto_generated": False,
                    "is_auto_corrected": False,
                    "is_auto_translated": False,
                    "translated_from": None,
                    "value": "",
                },
                "it": {
                    "is_auto_generated": False,
                    "is_auto_corrected": False,
                    "is_auto_translated": False,
                    "translated_from": None,
                    "value": "",
                },
            },
            "long": {
                "en": {
                    "is_auto_generated": False,
                    "is_auto_corrected": False,
                    "is_auto_translated": False,
                    "translated_from": None,
                    "value": "Long description",
                },
                "fr": {
                    "is_auto_generated": False,
                    "is_auto_corrected": False,
                    "is_auto_translated": False,
                    "translated_from": None,
                    "value": "",
                },
                "de": {
                    "is_auto_generated": False,
                    "is_auto_corrected": False,
                    "is_auto_translated": False,
                    "translated_from": None,
                    "value": "",
                },
                "it": {
                    "is_auto_generated": False,
                    "is_auto_corrected": False,
                    "is_auto_translated": False,
                    "translated_from": None,
                    "value": "",
                },
            },
        },
    }

    profile = PageProfile.from_json(data)

    assert profile.key == node_key
    assert profile.short_code == "CS-101"
    assert profile.is_visible is False
    assert profile.numeric_id.en == "101"
    assert profile.subtype.fr == "Cours"
    assert profile.external_key.en == "course-cs-101"
    assert profile.external_url.en == "https://example.org/cs101"
    assert profile.name.en.value == "Intro to CS"
    assert profile.name.fr.translated_from == "en"
    assert profile.description.short.fr.value == "Description courte"
    assert profile.description.long.en.value == "Long description"


def test_to_json(profile: PageProfile) -> None:
    profile.short_code = "CS-101"
    payload = profile.to_json()

    assert payload["key"]["institution_id"] == "EPFL"
    assert payload["short_code"] == "CS-101"
    assert payload["is_visible"] is True


def test_set_visibility(profile: PageProfile) -> None:
    profile.set_visibility(False)
    assert profile.is_visible is False

    profile.set_visibility(1)
    assert profile.is_visible is True


def test_numeric_id_getter_and_setter(profile: PageProfile) -> None:
    profile.set_numeric_id("en", "12345")
    assert profile.get_numeric_id("en") == "12345"


def test_subtype_getter_and_setter(profile: PageProfile) -> None:
    profile.set_subtype("fr", "Cours")
    assert profile.get_subtype("fr") == "Cours"


def test_external_key_getter_and_setter(profile: PageProfile) -> None:
    profile.set_external_key("en", "course-key")
    assert profile.get_external_key("en") == "course-key"


def test_external_url_getter_and_setter(profile: PageProfile) -> None:
    profile.set_external_url("en", "https://example.org/course")
    assert profile.get_external_url("en") == "https://example.org/course"


def test_get_name_returns_generated_text(profile: PageProfile) -> None:
    profile.set_name("en", "Introduction to CS")
    name = profile.get_name("en")

    assert name.value == "Introduction to CS"
    assert name.is_auto_generated is False
    assert name.is_auto_translated is False


def test_set_name_with_metadata(profile: PageProfile) -> None:
    profile.set_name(
        "fr",
        "Introduction à l'informatique",
        is_auto_generated=True,
        is_auto_corrected=True,
        is_auto_translated=True,
        translated_from="en",
    )

    name = profile.get_name("fr")
    assert name.value == "Introduction à l'informatique"
    assert name.is_auto_generated is True
    assert name.is_auto_corrected is True
    assert name.is_auto_translated is True
    assert name.translated_from == "en"


def test_get_description_returns_generated_text(profile: PageProfile) -> None:
    profile.set_description("short", "en", "Short text")
    description = profile.get_description("short", "en")

    assert description.value == "Short text"
    assert description.is_auto_generated is False
    assert description.is_auto_translated is False


def test_set_description_with_metadata(profile: PageProfile) -> None:
    profile.set_description(
        "medium",
        "fr",
        "Texte moyen",
        is_auto_generated=True,
        is_auto_corrected=False,
        is_auto_translated=True,
        translated_from="en",
    )

    description = profile.get_description("medium", "fr")
    assert description.value == "Texte moyen"
    assert description.is_auto_generated is True
    assert description.is_auto_corrected is False
    assert description.is_auto_translated is True
    assert description.translated_from == "en"


def test_set_description_invalid_size_raises(profile: PageProfile) -> None:
    with pytest.raises(ValueError, match="size must be one of: short, medium, long"):
        profile.set_description("tiny", "en", "oops")


def test_get_description_invalid_size_raises(profile: PageProfile) -> None:
    with pytest.raises(AttributeError):
        # get_description itself delegates to getattr, so invalid size currently raises AttributeError
        profile.get_description("tiny", "en")


def test_has_any_name_false_when_empty(profile: PageProfile) -> None:
    assert profile.has_any_name() is False


def test_has_any_name_true_when_present(profile: PageProfile) -> None:
    profile.set_name("de", "Einführung")
    assert profile.has_any_name() is True


def test_has_any_description_false_when_empty(profile: PageProfile) -> None:
    assert profile.has_any_description() is False


def test_has_any_description_true_for_any_size(profile: PageProfile) -> None:
    profile.set_description("long", "en", "Long text")
    assert profile.has_any_description() is True


def test_has_any_description_true_for_specific_size(profile: PageProfile) -> None:
    profile.set_description("short", "fr", "Texte court")
    assert profile.has_any_description(size="short") is True
    assert profile.has_any_description(size="medium") is False


def test_has_any_description_invalid_size_raises(profile: PageProfile) -> None:
    with pytest.raises(ValueError, match="size must be one of: short, medium, long"):
        profile.has_any_description(size="tiny")


def test_preferred_name_uses_language_order(profile: PageProfile) -> None:
    profile.set_name("fr", "Nom FR")
    profile.set_name("en", "Name EN")

    assert profile.preferred_name() == "Name EN"
    assert profile.preferred_name(("fr", "en")) == "Nom FR"


def test_preferred_name_returns_empty_string_when_missing(profile: PageProfile) -> None:
    assert profile.preferred_name() == ""


def test_preferred_description_uses_language_order(profile: PageProfile) -> None:
    profile.set_description("short", "fr", "Description FR")
    profile.set_description("short", "en", "Description EN")

    assert profile.preferred_description(size="short") == "Description EN"
    assert profile.preferred_description(size="short", preferred_languages=("fr", "en")) == "Description FR"


def test_preferred_description_returns_empty_string_when_missing(profile: PageProfile) -> None:
    assert profile.preferred_description(size="medium") == ""


def test_preferred_description_invalid_size_raises(profile: PageProfile) -> None:
    with pytest.raises(ValueError, match="size must be one of: short, medium, long"):
        profile.preferred_description(size="tiny")


def test_iter_languages(profile: PageProfile) -> None:
    assert list(profile.iter_languages()) == ["en", "fr", "de", "it"]
