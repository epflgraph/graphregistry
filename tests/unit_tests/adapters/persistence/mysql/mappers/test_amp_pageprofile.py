# tests/unit_tests/adapters/persistence/mysql/mappers/test_amp_pageprofile.py
"""Unit tests for the MySQL page profile mapper using synthetic row data."""
from __future__ import annotations

from graphregistry.adapters.persistence.mysql.mappers.amp_pageprofile import MySQLPageProfileMapper
from graphregistry.domain.models.entities.mdl_base import NodeKey
from graphregistry.domain.models.entities.mdl_pageprofile import PageProfile


class TestMySQLPageProfileMapper:
    def test_from_empty_row_returns_default_profile(self) -> None:
        key = NodeKey(object_type="Course", object_id="CS-433")
        profile = MySQLPageProfileMapper.from_row(None, node_key=key)
        assert profile.key == key
        assert profile.short_code == ""
        assert profile.is_visible is True

    def test_from_row_populates_basic_fields(self) -> None:
        key = NodeKey(object_type="Course", object_id="CS-433")
        row = {
            "short_code": "ML",
            "is_visible": 0,
            "numeric_id_en": "123",
            "subtype_en": "Course",
            "external_key_en": "ext-123",
            "external_url_en": "https://example.com",
        }
        profile = MySQLPageProfileMapper.from_row(row, node_key=key)
        assert profile.short_code == "ML"
        assert profile.is_visible is False
        assert profile.numeric_id.get_value("en") == "123"
        assert profile.subtype.get_value("en") == "Course"
        assert profile.external_key.get_value("en") == "ext-123"
        assert profile.external_url.get_value("en") == "https://example.com"

    def test_from_row_populates_generated_name(self) -> None:
        key = NodeKey(object_type="Course", object_id="CS-433")
        row = {
            "name_en_value": "Machine Learning",
            "name_en_is_auto_generated": 1,
            "name_fr_value": "Apprentissage automatique",
        }
        profile = MySQLPageProfileMapper.from_row(row, node_key=key)
        assert profile.name.get_value("en") == "Machine Learning"
        assert profile.name.get("en").is_auto_generated is True
        assert profile.name.get_value("fr") == "Apprentissage automatique"

    def test_from_row_populates_descriptions(self) -> None:
        key = NodeKey(object_type="Course", object_id="CS-433")
        row = {
            "description_short_en_value": "Short",
            "description_long_en_value": "Long description",
            "description_long_en_translated_from": "fr",
        }
        profile = MySQLPageProfileMapper.from_row(row, node_key=key)
        assert profile.description.short.get_value("en") == "Short"
        assert profile.description.long.get_value("en") == "Long description"
        assert profile.description.long.get("en").translated_from == "fr"

    def test_to_row_omits_empty_values(self) -> None:
        key = NodeKey(object_type="Course", object_id="CS-433")
        profile = PageProfile(key=key, short_code="ML")
        profile.name.set("en", "Machine Learning", is_auto_generated=True)
        row = MySQLPageProfileMapper.to_row(profile)
        assert row["short_code"] == "ML"
        assert row["name_en_value"] == "Machine Learning"
        assert row["name_en_is_auto_generated"] == 1
        assert row["record_deleted"] == 0
        assert "name_fr_value" not in row
