# tests/unit_tests/common/test_config.py
"""Unit tests for configuration loaders."""
from __future__ import annotations

from graphregistry.common.config import IndexConfig


def test_index_config_exposes_edge_selection_contexts() -> None:
    """object-selection.edges from config_index.json is parsed into a sorted
    (from_type, to_type) -> context lookup used during index builds.
    """
    cfg = IndexConfig()

    # The raw selection list is preserved in config file order.
    assert cfg.settings["edge_selection"][0] == ["Concept", "Category", "ontology tree"]
    assert ["Course", "Person", "teacher"] in cfg.settings["edge_selection"]
    assert ["Lecture", "Course", "part of"] in cfg.settings["edge_selection"]

    # Contexts are keyed by the alphabetically sorted pair (as a tuple)
    # so both directions resolve to the same canonical context.
    contexts = cfg.settings["edge_selection_contexts"]
    assert contexts[("Category", "Concept")] == "ontology tree"
    assert contexts[("Course", "Person")] == "teacher"
    assert contexts[("Course", "Lecture")] == "part of"
    assert contexts[("Lecture", "Widget")] == "part of"
