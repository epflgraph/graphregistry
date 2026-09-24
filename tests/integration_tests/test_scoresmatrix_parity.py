# graphregistry/tests/integration_tests/test_scoresmatrix_parity.py
"""Read-only parity tests: legacy scores matrix helpers vs
MySQLScoresMatrixRepository against a live database.

The table naming is compared against the legacy
get_scores_matrix_table_name function over every configured family, and the
calculation and consolidation are exercised in eval mode only, which issues
reads but no writes. Skips automatically when the database is not reachable.

Run with:  pytest tests/integration_tests/test_scoresmatrix_parity.py -v
"""
from __future__ import annotations
import pytest
from graphregistry.adapters.persistence.mysql.repositories.resolvers import DefaultSchemaResolver
from graphregistry.adapters.persistence.mysql.repositories.rpo_scoresmatrix import MySQLScoresMatrixRepository
from graphregistry.common.config import GlobalConfig, ScoresConfig
from graphregistry.domain.models.pipeline.mdl_scores import ScoreConsolidationParams
from graphregistry.domain.models.values.mdl_typepairs import EdgeTypePair

# Engine name used by the legacy pipeline for all scores matrix queries.
ENGINE_NAME = "coresrv"

#================================================================#
# Function Group: Pytest fixtures                                #
#================================================================#

# Public Function: Build a database client, skipping the module when unreachable.
@pytest.fixture(scope="module")
def db():
    try:
        from graphdb.core.graphdb import GraphDB
        client = GraphDB()
    except Exception as exc:  # pragma: no cover - depends on the environment
        pytest.skip(f"Database not reachable, skipping parity test: {exc}")
        return
    return client

# Public Function: Build the typed repository over the live database.
@pytest.fixture(scope="module")
def repo(db) -> MySQLScoresMatrixRepository:
    resolver = DefaultSchemaResolver(engine_name=ENGINE_NAME, glbcfg=GlobalConfig())
    return MySQLScoresMatrixRepository(
        db              = db,
        schema_resolver = resolver,
        global_config   = GlobalConfig(),
        scores_config   = ScoresConfig(),
    )

# Public Function: Import the legacy table naming function as the oracle.
@pytest.fixture(scope="module")
def legacy_table_name():
    try:
        from graphregistry.application.core.cor_registry import get_scores_matrix_table_name
    except Exception as exc:  # pragma: no cover - depends on the environment
        pytest.skip(f"Legacy cor_registry could not be imported: {exc}")
        return
    return get_scores_matrix_table_name

#================================================================#
# Function Group: Parity tests                                   #
#================================================================#

# Public Function: Verify the matrix table naming matches the legacy function.
def test_matrix_table_names_match_legacy(repo, legacy_table_name) -> None:
    scores_config = ScoresConfig()

    # Every configured education and research family resolves identically.
    pairs = list(scores_config.settings['scored_edge_tuple_to_class_mapping'].keys())

    # Ontology families and same-type ontology pairs are covered as well.
    pairs += [('Category', 'Person'), ('Concept', 'Course'), ('Curated area', 'Publication'),
              ('Category', 'Category'), ('Concept', 'Concept'), ('Curated area', 'Curated area')]
    for from_type, to_type in pairs:
        for kind in ('GBC', 'AS'):
            expected = legacy_table_name(from_type, to_type, gbc_or_as=kind)
            actual = repo._matrix_table_name(EdgeTypePair(from_object_type=from_type, to_object_type=to_type), kind)
            assert actual == expected, f"Table name mismatch for ({from_type}, {to_type}, {kind})"

# Public Function: Verify the calculation eval runs against the live schema.
def test_calculate_matrix_eval_against_live_schema(repo) -> None:
    scores_config = ScoresConfig()
    from_type, to_type = sorted(scores_config.settings['scored_edge_tuple_to_class_mapping'].keys())[0]

    # Eval mode reads the estimated pair count without writing.
    stats = repo.calculate_matrix(
        type_pair = EdgeTypePair(from_object_type=from_type, to_object_type=to_type),
        params    = ScoreConsolidationParams(),
        actions   = ('eval',),
    )
    assert stats is not None
    assert stats.rows_flagged >= 0
    assert "ScoresMatrix" in stats.target

# Public Function: Verify the ontology exclusion matches the legacy behavior.
def test_calculate_matrix_ontology_pair_excluded(repo) -> None:
    stats = repo.calculate_matrix(
        type_pair = EdgeTypePair(from_object_type="Concept", to_object_type="Person"),
        params    = ScoreConsolidationParams(),
        actions   = ('eval', 'commit'),
    )
    assert stats is None

# Public Function: Verify the consolidation eval runs the average check only.
def test_consolidate_matrix_eval_runs_check_read(repo) -> None:
    scores_config = ScoresConfig()

    # Pick a non-ontology family so the adjusted-scores branch runs its
    # average existence check in eval mode.
    non_ontology = [
        (from_type, to_type) for (from_type, to_type) in scores_config.settings['scored_edge_tuple_to_class_mapping'].keys()
        if from_type not in ('Category', 'Concept', 'Curated area') and to_type not in ('Category', 'Concept', 'Curated area')
    ]
    if not non_ontology:
        pytest.skip("No non-ontology scores families configured.")
    from_type, to_type = non_ontology[0]
    repo.consolidate_matrix(
        type_pair = EdgeTypePair(from_object_type=from_type, to_object_type=to_type),
        params    = ScoreConsolidationParams(),
        actions   = ('eval',),
    )
