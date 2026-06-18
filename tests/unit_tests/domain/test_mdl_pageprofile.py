# tests/unit_tests/domain/test_mdl_pageprofile.py
"""Unit tests for the PageProfile value object."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from graphregistry.domain.models.entities.mdl_base import NodeKey
from graphregistry.domain.models.entities.mdl_pageprofile import PageProfile


class TestPageProfile:
    def test_page_profile_requires_node_key(self) -> None:
        key = NodeKey(institution_id="EPFL", object_type="Course", object_id="CS-433")
        profile = PageProfile(key=key)
        assert profile.key == key

    def test_page_profile_key_must_be_node_key(self) -> None:
        with pytest.raises(ValidationError):
            PageProfile(key="invalid-key")  # type: ignore[arg-type]

    def test_page_profile_multilingual_fields(self) -> None:
        key = NodeKey(institution_id="EPFL", object_type="Course", object_id="CS-433")
        profile = PageProfile(key=key, short_code="ML")
        profile.name.set("en", "Machine Learning", is_auto_generated=True)
        profile.description.long.set("en", "A course about machine learning.")
        profile.external_url.set("en", "https://edu.epfl.ch/coursebook/machine-learning")

        assert profile.short_code == "ML"
        assert profile.name.get_value("en") == "Machine Learning"
        assert profile.name.get("en").is_auto_generated is True
        assert profile.description.long.get_value("en") == "A course about machine learning."
        assert profile.external_url.get_value("en") == "https://edu.epfl.ch/coursebook/machine-learning"

    def test_page_profile_json_roundtrip(self) -> None:
        key = NodeKey(institution_id="EPFL", object_type="Course", object_id="CS-433")
        profile = PageProfile(key=key, short_code="ML")
        profile.name.set("en", "Machine Learning")
        rebuilt = PageProfile.from_json(profile.to_json())
        assert rebuilt.short_code == "ML"
        assert rebuilt.name.get_value("en") == "Machine Learning"
