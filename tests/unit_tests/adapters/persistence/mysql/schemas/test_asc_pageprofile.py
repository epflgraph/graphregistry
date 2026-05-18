from __future__ import annotations

from graphregistry.adapters.persistence.mysql.schemas.asc_pageprofile import PAGE_PROFILE_COLUMNS


def test_page_profile_columns_contains_required_examples_and_no_duplicates() -> None:
    assert "short_code" in PAGE_PROFILE_COLUMNS
    assert "name_en_value" in PAGE_PROFILE_COLUMNS
    assert "description_long_it_value" in PAGE_PROFILE_COLUMNS
    assert "is_visible" in PAGE_PROFILE_COLUMNS
    assert len(PAGE_PROFILE_COLUMNS) == len(set(PAGE_PROFILE_COLUMNS))
