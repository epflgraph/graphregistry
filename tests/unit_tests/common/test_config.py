# graphregistry/tests/unit_tests/common/test_config.py
"""Unit tests for configuration loaders."""
from __future__ import annotations
import json
from graphregistry.common.config import IndexConfig, ScoresConfig
from graphregistry.common.dbstruct import DynamicSQL
from graphregistry.common.paths import DATABASE_SYSTEM_DATATYPES_PATH

# Public Method: Verify edge-selection contexts are parsed into a sorted lookup.
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

# Public Method: Verify mixed scoring tuples are exposed as configured.
def test_scores_config_exposes_mixed_scoring_tuples() -> None:
    """mixed-scoring-tuples from config_scores.json is parsed into tuples."""
    cfg = ScoresConfig()

    # Raw config order is preserved.
    assert cfg.settings["mixed_scoring_tuples"][0] == ("Category", "Category")
    assert ("Person", "Unit") in cfg.settings["mixed_scoring_tuples"]
    # The raw list contains only the pairs declared in the file.
    assert ("Unit", "Person") not in cfg.settings["mixed_scoring_tuples"]

# Public Method: Verify MIX edge pairs are expanded to both directions.
def test_dynamic_sql_expands_mixed_scoring_tuples_to_both_directions() -> None:
    """DynamicSQL exposes the configured MIX pairs plus their reverses."""
    dynsql = DynamicSQL(db=None)

    # Configured MIX pairs and their reverses must both be present.
    assert ("Person", "Unit") in dynsql.doclink_types_mix
    assert ("Unit", "Person") in dynsql.doclink_types_mix
    assert ("Course", "Person") in dynsql.doclink_types_mix
    assert ("Person", "Course") in dynsql.doclink_types_mix
    # Non-mixed pairs should not be in the MIX list.
    assert ("Course", "Lecture") not in dynsql.doclink_types_mix
    assert ("Lecture", "Course") not in dynsql.doclink_types_mix

# Public Method: Verify system_datatypes.json exists with a flat schema.
def test_system_datatypes_path_exists_and_is_flat() -> None:
    """system_datatypes.json exists and has the expected flat {field: type} shape."""
    # The file must exist and be readable as JSON.
    assert DATABASE_SYSTEM_DATATYPES_PATH.exists()

    # Every entry must be a flat field-name -> sql-type mapping.
    raw = json.loads(DATABASE_SYSTEM_DATATYPES_PATH.read_text(encoding="utf-8"))
    assert isinstance(raw, dict)
    for field, datatype in raw.items():
        assert isinstance(field, str)
        assert isinstance(datatype, str)

# Public Method: Verify DynamicSQL resolves SQL types from system_datatypes.json.
def test_dynamic_sql_resolves_system_datatypes() -> None:
    """Fields defined in system_datatypes.json are converted to SQL types."""
    dynsql = DynamicSQL(db=None)

    # Core id fields and index flags come from system_datatypes.json.
    datatypes = dynsql.get_datatypes_from_fields(
        ["doc_type", "doc_id", "row_id", "include_code_in_name"]
    )
    assert "varchar(32) NOT NULL" in datatypes
    assert "varchar(255) NOT NULL" in datatypes
    assert "bigint(20) unsigned NOT NULL AUTO_INCREMENT" in datatypes
    assert "tinyint(1) NULL DEFAULT NULL" in datatypes

# Public Method: Verify fallback to config_index.json for undefined system types.
def test_dynamic_sql_falls_back_to_index_datatypes() -> None:
    """Fields not in system_datatypes.json fall back to config_index.json data-types."""
    dynsql = DynamicSQL(db=None)

    # "year" is defined in config_index.json, not in system_datatypes.json.
    datatypes = dynsql.get_datatypes_from_fields(["year"])
    assert datatypes == ["MEDIUMINT UNSIGNED"]
