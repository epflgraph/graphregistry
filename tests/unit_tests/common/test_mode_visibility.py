# graphregistry/tests/unit_tests/common/test_mode_visibility.py
"""Unit tests for the execution-mode visibility badges in config and entrypoints."""
from __future__ import annotations
from graphregistry.common.config import GlobalConfig, mode_badge, mode_border_style, mode_note
from graphregistry.entrypoints.api.main import create_app

# Public Method: Verify every known mode gets a non-empty badge containing its
# mode name, and unknown modes get the question-mark fallback. The specific
# emoji and casing are styling choices and intentionally not asserted.
def test_mode_badge_covers_known_modes() -> None:
    for mode in ("dev", "test", "prod"):
        assert mode in mode_badge(mode).lower()
        assert len(mode_badge(mode).strip()) > 0
    assert "❓" in mode_badge("staging")

# Public Method: Verify unknown modes fall back to a visible warning note.
def test_mode_note_warns_for_unknown_mode() -> None:
    assert "unrecognized" in mode_note("staging")
    assert "staging" in mode_note("staging")
    assert "_1_DEV_" in mode_note("dev")
    assert "production" in mode_note("prod").lower()

# Public Method: Verify border styles are non-empty for known modes, with a
# neutral white fallback for unknown modes. Colors are styling choices and
# intentionally not asserted.
def test_mode_border_style_matches_mode() -> None:
    for mode in ("dev", "test", "prod"):
        assert len(mode_border_style(mode).strip()) > 0
    assert mode_border_style("staging") == "white"

# Public Method: Verify GlobalConfig builds a mode label from the loaded config.
def test_global_config_mode_label_matches_configured_mode() -> None:
    cfg = GlobalConfig()
    assert mode_badge(cfg.mysql_execution_mode) in cfg.mode_label
    assert mode_note(cfg.mysql_execution_mode) in cfg.mode_label

# Public Method: Verify the Swagger header keeps a clean title and carries the
# mode exactly once, in the description.
def test_create_app_openapi_header_carries_mode() -> None:
    app = create_app()
    info = app.openapi()["info"]
    configured_mode = GlobalConfig().mysql_execution_mode
    assert info["title"] == GlobalConfig().api_title
    assert info["summary"] == GlobalConfig().api_summary
    assert info["description"].count(mode_note(configured_mode)) == 1
    assert mode_badge(configured_mode) in info["description"]
    assert info["summary"] not in info["description"]
