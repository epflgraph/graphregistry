# tests/unit_tests/common/test_config.py
"""Unit tests for configuration loaders."""
from __future__ import annotations

from graphregistry.common.config import IndexConfig, ScoresConfig
from graphregistry.common.dbstruct import DynamicSQL


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


def test_scores_config_exposes_mixed_scoring_tuples() -> None:
    """mixed-scoring-tuples from config_scores.json is parsed into tuples."""
    cfg = ScoresConfig()

    # Raw config order is preserved.
    assert cfg.settings["mixed_scoring_tuples"][0] == ("Category", "Category")
    assert ("Person", "Unit") in cfg.settings["mixed_scoring_tuples"]
    # The raw list contains only the pairs declared in the file.
    assert ("Unit", "Person") not in cfg.settings["mixed_scoring_tuples"]


def test_dynamic_sql_expands_mixed_scoring_tuples_to_both_directions() -> None:
    """DynamicSQL exposes the configured MIX pairs plus their reverses."""
    dynsql = DynamicSQL(db=None)

    assert ("Person", "Unit") in dynsql.doclink_types_mix
    assert ("Unit", "Person") in dynsql.doclink_types_mix
    assert ("Course", "Person") in dynsql.doclink_types_mix
    assert ("Person", "Course") in dynsql.doclink_types_mix
    # Non-mixed pairs should not be in the MIX list.
    assert ("Course", "Lecture") not in dynsql.doclink_types_mix
    assert ("Lecture", "Course") not in dynsql.doclink_types_mix
